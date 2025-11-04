import pytest
import asyncio
from mictlanx.retry import RetryPolicy, raf
from option import Result, Ok, Err

# --- A helper class to mock a function that can fail ---

class MockTarget:
    """
    A mockable class that simulates a function that can fail
    a specific number of times before succeeding.
    """
    def __init__(self, fail_until_attempt: int):
        self.fail_until_attempt = fail_until_attempt
        self.call_count = 0

    def sync_call(self) -> Result[str, Exception]:
        """A mock synchronous function."""
        self.call_count += 1
        if self.call_count < self.fail_until_attempt:
            raise Exception("BOOOM!")
        return Ok("Success")
    
    async def async_call(self) -> Result[str, Exception]:
        """A mock asynchronous function."""
        self.call_count += 1
        await asyncio.sleep(0.01) # Simulate async work
        if self.call_count < self.fail_until_attempt:
            raise Exception("BOOOM!")
        return Ok("Success")

# --- Improved Tests ---

@pytest.mark.asyncio
async def test_raf_always_fails():
    """
    Tests that 'raf' correctly fails after all retries are exhausted.
    (This is the improved version of your original test).
    """
    # This target will fail on calls 1, 2, and 3
    target = MockTarget(fail_until_attempt=99) 
    
    # Capture side-effects instead of printing
    attempt_log = []
    error_log = []
    
    policy = RetryPolicy(
        retries=3, 
        initial_delay=0.01,  # Use small delays for tests
        backoff_factor=0.5, 
        max_delay=0.1,
        jitter=False, 
        retry_on=lambda e: isinstance(e, Exception)
    )
    
    res = await raf(
        func=target.sync_call,
        policy=policy,
        on_attempt=lambda i: attempt_log.append(i),
        on_error=lambda i, e: error_log.append((i, e))
    )
    
    # --- Assertions ---
    # 1. Check the final result
    assert res.is_err, "Expected the final result to be an Err"
    assert isinstance(res.unwrap_err(), Exception)
    # Check that the original error message is *contained* in the final error
    assert "BOOOM!" in str(res.unwrap_err())
    
    # 2. Check the call counts
    assert target.call_count == 3, "Expected the function to be called 3 times"
    
    # 3. Check the callback logs
    # on_attempt is called for all attempts (1, 2, and 3)
    assert len(attempt_log) == 3
    assert attempt_log == [1, 2, 3] 
    
    # on_error is called for all failed attempts (1, 2, and 3)
    assert len(error_log) == 3 

@pytest.mark.asyncio
async def test_raf_succeeds_on_retry():
    """
    Tests that 'raf' stops and returns Ok() once the function succeeds.
    """
    # This target will fail on calls 1 & 2, but succeed on call 3
    target = MockTarget(fail_until_attempt=3) 
    
    attempt_log = []
    error_log = []
    
    policy = RetryPolicy(
        retries=5, # Give it plenty of retries
        initial_delay=0.01, 
        backoff_factor=0.5,
        retry_on=lambda e: isinstance(e, Exception)
    )
    
    res = await raf(
        func=target.sync_call,
        policy=policy,
        on_attempt=lambda i: attempt_log.append(i),
        on_error=lambda i, e: error_log.append((i, e))
    )
    
    # --- Assertions ---
    # 1. Check the final result
    assert res.is_ok, "Expected the final result to be Ok"
    assert res.unwrap() == "Success"
    
    # 2. Check the call count (it should stop after the first success)
    assert target.call_count == 3
    
    # 3. Check the callback logs
    # on_attempt is called for attempts 1, 2, and 3
    assert attempt_log == [1, 2, 3]
    # on_error is called for failed attempts 1 and 2
    assert len(error_log) == 2
    assert error_log[0][0] == 1 # Check attempt number for error 1
    assert error_log[1][0] == 2 # Check attempt number for error 2

@pytest.mark.asyncio
async def test_raf_succeeds_first_try():
    """
    Tests that 'raf' succeeds immediately and doesn't retry.
    """
    # This target will succeed on call 1
    target = MockTarget(fail_until_attempt=1) 
    
    attempt_log = []
    error_log = []

    policy = RetryPolicy(retries=3, initial_delay=0.01, retry_on=lambda e: isinstance(e, Exception))

    res = await raf(
        func=target.sync_call,
        policy=policy,
        on_attempt=lambda i: attempt_log.append(i),
        on_error=lambda i, e: error_log.append((i, e))
    )
    
    # --- Assertions ---
    assert res.is_ok
    assert res.unwrap() == "Success"
    assert target.call_count == 1   # Only called once
    # on_attempt is called once (for attempt 1)
    assert attempt_log == [1]
    # No errors
    assert len(error_log) == 0

@pytest.mark.asyncio
async def test_raf_with_async_func():
    """
    Tests that 'raf' also works correctly with an async function.
    """
    # Fail on 1 & 2, succeed on 3
    target = MockTarget(fail_until_attempt=3) 
    
    attempt_log = []
    error_log = []
    policy = RetryPolicy(retries=5, initial_delay=0.01, retry_on=lambda e: isinstance(e, Exception))

    res = await raf(
        func=target.async_call, # Pass the async function
        policy=policy,
        on_attempt=lambda i: attempt_log.append(i),
        on_error=lambda i, e: error_log.append((i, e))
    )
    
    # --- Assertions ---
    assert res.is_ok
    assert res.unwrap() == "Success"
    assert target.call_count == 3
    # on_attempt is called for attempts 1, 2, and 3
    assert attempt_log == [1, 2, 3]
    # on_error is called for failed attempts 1 and 2
    assert len(error_log) == 2