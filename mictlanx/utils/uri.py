from __future__ import annotations
from typing import List, Tuple, Dict, Iterable, Optional
from mictlanx.services import AsyncRouter


class MictlanXURI:
    @staticmethod
    def _parse_bool(s: str, default: bool = False) -> bool:
        if s is None:
            return default
        s = s.strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
        return default

    @staticmethod
    def _split_once(s: str, sep: str) -> Tuple[str, Optional[str]]:
        i = s.find(sep)
        if i == -1:
            return s, None
        return s[:i], s[i + len(sep):]

    @staticmethod
    def _normalize_uri(uri: str) -> str:
        # tolerate `mictlanx:://` and fix to `mictlanx://`
        return uri.replace(":://", "://", 1).strip()

    @staticmethod
    def _split_routers_and_query(rest: str) -> Tuple[str, Optional[str]]:
        """
        Split `rest` (after 'mictlanx://') at the *first* occurrence of either '/'
        or '?' so router list never includes a slash.
        Returns (routers_part, tail_without_separator_or_None).
        """
        i_q = rest.find("?")
        i_s = rest.find("/")
        idxs = [i for i in (i_q, i_s) if i != -1]
        if not idxs:
            return rest, None
        i = min(idxs)
        return rest[:i], rest[i + 1 :]  # drop the separator

    @staticmethod
    def _parse_query(q: str) -> Dict[str, str]:
        # Accept both `?a=1&b=2` and `/a=1&b=2?c=3`
        if not q:
            return {}
        q = q.lstrip("/?")
        q = q.replace("?", "&")  # normalize secondary '?' into '&'
        params: Dict[str, str] = {}
        for part in q.split("&"):
            if not part:
                continue
            k, v = MictlanXURI._split_once(part, "=")
            if v is None:
                v = ""
            params[k] = v
        return params

    @staticmethod
    def _parse_router_spec(spec: str) -> Tuple[str, str, int]:
        """
        Accepts:
          - router_id@host:port   (preferred)
          - router_id:host:port   (legacy)
        """
        spec = spec.strip()
        if not spec:
            raise ValueError("no routers")

        if "@" in spec:
            rid, hostport = MictlanXURI._split_once(spec, "@")
            host, port_s = MictlanXURI._split_once(hostport, ":")
        else:
            parts = spec.split(":")
            if len(parts) < 3:
                raise ValueError(
                    f"invalid router spec `{spec}`; expected router_id@host:port or router_id:host:port"
                )
            rid = parts[0]
            host = ":".join(parts[1:-1])
            port_s = parts[-1]

        rid = rid.strip()
        host = host.strip()
        if not rid:
            raise ValueError(f"router_id missing in `{spec}`")
        if not host:
            raise ValueError(f"host missing in `{spec}`")
        try:
            port = int(port_s)
        except Exception:
            raise ValueError(f"invalid port in `{spec}`: {port_s!r}")
        if not (1 <= port <= 65535):
            raise ValueError(f"port out of range in `{spec}`: {port}")
        return rid, host, port

    @staticmethod
    def parse_mictlanx_uri(uri: str) -> List[AsyncRouter]:
        """
        Parse a mictlanx:// URI and return a list of AsyncRouter objects.
        Global query params apply to all routers.
        """
        uri = MictlanXURI._normalize_uri(uri)
        if not uri.startswith("mictlanx://"):
            raise ValueError("URI must start with mictlanx://")

        rest = uri[len("mictlanx://"):]
        routers_part, tail = MictlanXURI._split_routers_and_query(rest)

        params = MictlanXURI._parse_query(tail or "")
        protocol    = params.get("protocol", "http")
        api_version = int(params.get("api_version", "4"))
        http2       = MictlanXURI._parse_bool(params.get("http2"), False)

        # Detect empty router specs explicitly to satisfy your test
        raw_specs = [p.strip() for p in routers_part.split(",")]
        if any(s == "" for s in raw_specs):
            raise ValueError("no routers")

        routers: List[AsyncRouter] = []
        for spec in raw_specs:
            rid, host, port = MictlanXURI._parse_router_spec(spec)
            routers.append(
                AsyncRouter(
                    router_id=rid,
                    ip_addr=host,
                    port=port,
                    protocol=protocol,
                    http2=http2,
                    api_version=api_version,
                )
            )
        if not routers:
            raise ValueError("no routers found in URI")
        return routers

    @staticmethod
    def build_mictlanx_uri(routers: Iterable[AsyncRouter]) -> str:
        """
        Build a canonical mictlanx:// URI from AsyncRouter objects.
        Uses the first router's global settings for query params.
        """
        routers = list(routers)
        if not routers:
            raise ValueError("need at least one router to build URI")

        r0 = routers[0]
        router_list = ",".join(f"{r.router_id}@{r.ip_addr}:{r.port}" for r in routers)
        q = f"protocol={r0.protocol}&api_version={int(r0.api_version)}&http2={'1' if r0.http2 else '0'}"
        # If you want to enforce a mandatory slash before the query, use: f"mictlanx://{router_list}/{q}"
        return f"mictlanx://{router_list}/?{q}"
