from typing import List, Any
import asyncio
from mictlanx import errors as EX
from mictlanx.interfaces.bulk import BulkPutResponse, BulkPutSuccess, BulkPutFailure
from mictlanx.logger import Log
from option import Result, Ok, Err
from tqdm.asyncio import tqdm_asyncio as tqdm

class _BulkJob:
    """
    Internal class to manage the state of a background bulk operation.
    """
    def __init__(self, bulk_id: str, max_concurrency: int, logger: Log):
        self.bulk_id = bulk_id
        self.logger = logger
        self.tasks: List[asyncio.Task] = []
        self.lock = asyncio.Lock()  # Protects the tasks list
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._is_waiting = False # Flag to prevent adding tasks while waiting
        
    def is_finalized(self) -> bool:
        """Checks if the job is already being waited on."""
        return self._is_waiting
        
    async def add_tasks(self, tasks: List[asyncio.Task]):
        """Adds new tasks to the job's list."""
        async with self.lock:
            if self._is_waiting:
                raise EX.MictlanXError(f"Cannot add tasks to bulk job '{self.bulk_id}' as it is already being awaited.")
            self.tasks.extend(tasks)
            
    async def wait_for_completion(self) -> BulkPutResponse:
        """Waits for all tasks in the job to complete."""
        async with self.lock:
            if self._is_waiting:
                raise EX.MictlanXError(f"Bulk job '{self.bulk_id}' is already being awaited elsewhere.")
            if not self.tasks:
                # No tasks, return empty response
                return BulkPutResponse(successes=[], failures=[])
            self._is_waiting = True
            
        successes: List[BulkPutSuccess] = []
        failures: List[BulkPutFailure] = []

        self.logger.info(f"Awaiting completion of {len(self.tasks)} tasks for bulk_id '{self.bulk_id}'...")
        
        # Wait for all tasks to complete and process results
        pbar = tqdm(total=len(self.tasks), desc=f"Awaiting Bulk Job '{self.bulk_id}'")
        for coro in asyncio.as_completed(self.tasks):
            result = await coro
            
            if isinstance(result, Exception):
                # This is an unexpected exception from _process_item wrapper itself
                # We can't identify the item, so we log it.
                self.logger.error(f"Unexpected exception during bulk 'await': {result}", exc_info=True)
            
            elif result.is_ok:
                item  = result.unwrap()
                successes.append({"item": item})
            else:
                (item, err) = result.unwrap_err()
                failures.append({"item": item, "error": err})
            
            pbar.update(1)

        pbar.close()
        self.logger.info(f"Bulk job '{self.bulk_id}' complete. Success: {len(successes)}, Failure: {len(failures)}")
        return BulkPutResponse(successes=successes, failures=failures)

