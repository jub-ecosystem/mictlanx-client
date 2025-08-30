import pytest
from mictlanx.retry import RetryPolicy,raf
from option import Result,Ok,Err


def f_exception()->Result[str, Exception]:
    raise Exception("BOOOM!") 

@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_raf():
    policy = RetryPolicy(retries=3, initial_delay=1, backoff_factor=.3, max_delay=10)
    res = await raf(
        func= f_exception,
        policy=policy,
        on_attempt=lambda i: print(f"Attemp {i}"),
        on_error= lambda i,e: print(f"Failed {e} ")
    )
    print(res)
    