from __future__ import annotations
import io
from typing import Any, Dict, List

from option import Result, Ok, Err
from mictlanx.apac.backends.base import AbstractStorageBackend


class GoogleDriveBackend(AbstractStorageBackend):
    """
    Storage backend backed by a Google Drive folder.

    Requires: google-api-python-client, google-auth-httplib2, google-auth-oauthlib
        pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib

    Objects are stored as Drive files under `root_folder_id/bucket_id/key`.
    A subfolder is created per bucket_id on first use.
    """

    def __init__(
        self,
        backend_id: str,
        root_folder_id: str,
        credentials_path: str = "credentials.json",
        token_path: str = "token.json",
    ) -> None:
        self._id = backend_id
        self._root_folder_id = root_folder_id

        try:
            from googleapiclient.discovery import build
            from google.oauth2.credentials import Credentials
            from google_auth_oauthlib.flow import InstalledAppFlow

            SCOPES = ["https://www.googleapis.com/auth/drive"]
            creds = None
            import os
            if os.path.exists(token_path):
                creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            if not creds or not creds.valid:
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
                creds = flow.run_local_server(port=0)
                with open(token_path, "w") as f:
                    f.write(creds.to_json())
            self._service = build("drive", "v3", credentials=creds)
        except ImportError:
            raise ImportError(
                "Google Drive SDK required: "
                "pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib"
            )

        self._bucket_folder_cache: Dict[str, str] = {}

    @property
    def backend_id(self) -> str:
        return self._id

    def _get_or_create_folder(self, name: str, parent_id: str) -> str:
        query = (
            f"name='{name}' and mimeType='application/vnd.google-apps.folder' "
            f"and '{parent_id}' in parents and trashed=false"
        )
        results = self._service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        if files:
            return files[0]["id"]
        meta = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        folder = self._service.files().create(body=meta, fields="id").execute()
        return folder["id"]

    def _bucket_folder_id(self, bucket_id: str) -> str:
        if bucket_id not in self._bucket_folder_cache:
            self._bucket_folder_cache[bucket_id] = self._get_or_create_folder(
                bucket_id, self._root_folder_id
            )
        return self._bucket_folder_cache[bucket_id]

    def _find_file_id(self, bucket_id: str, key: str) -> str | None:
        folder_id = self._bucket_folder_id(bucket_id)
        query = f"name='{key}' and '{folder_id}' in parents and trashed=false"
        results = self._service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        return files[0]["id"] if files else None

    async def put(
        self,
        bucket_id: str,
        key: str,
        value: bytes,
        tags: Dict[str, str] = {},
        content_type: str = "application/octet-stream",
    ) -> Result[Dict[str, Any], Exception]:
        try:
            from googleapiclient.http import MediaIoBaseUpload

            folder_id = self._bucket_folder_id(bucket_id)
            existing_id = self._find_file_id(bucket_id, key)
            media = MediaIoBaseUpload(io.BytesIO(value), mimetype=content_type)

            if existing_id:
                file = self._service.files().update(
                    fileId=existing_id, media_body=media, fields="id"
                ).execute()
            else:
                meta = {"name": key, "parents": [folder_id]}
                file = self._service.files().create(
                    body=meta, media_body=media, fields="id"
                ).execute()
            return Ok({"file_id": file["id"], "key": key, "bucket_id": bucket_id})
        except Exception as e:
            return Err(e)

    async def get(self, bucket_id: str, key: str) -> Result[bytes, Exception]:
        try:
            from googleapiclient.http import MediaIoBaseDownload

            file_id = self._find_file_id(bucket_id, key)
            if not file_id:
                return Err(FileNotFoundError(f"{bucket_id}/{key} not found in Google Drive"))
            request = self._service.files().get_media(fileId=file_id)
            buf = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return Ok(buf.getvalue())
        except Exception as e:
            return Err(e)

    async def list(self, bucket_id: str) -> Result[List[str], Exception]:
        try:
            folder_id = self._bucket_folder_id(bucket_id)
            query = f"'{folder_id}' in parents and trashed=false"
            results = self._service.files().list(q=query, fields="files(name)").execute()
            return Ok([f["name"] for f in results.get("files", [])])
        except Exception as e:
            return Err(e)

    async def delete(self, bucket_id: str, key: str) -> Result[bool, Exception]:
        try:
            file_id = self._find_file_id(bucket_id, key)
            if not file_id:
                return Ok(False)
            self._service.files().delete(fileId=file_id).execute()
            return Ok(True)
        except Exception as e:
            return Err(e)

    async def health(self) -> bool:
        try:
            self._service.files().list(pageSize=1, fields="files(id)").execute()
            return True
        except Exception:
            return False
