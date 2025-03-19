from typing import Callable, TypeVar, Awaitable,Dict,List,Any
from option import Result
import asyncio

X = TypeVar("X")  # The successful return type
E = TypeVar("E", bound=Exception)  # The error type

async def raf(
    func: Callable[..., Awaitable[Result[X, E]]],
    retries: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    fargs:List[Any]=[],
    fkwargs:Dict[str,Any] ={}
) -> Result[X, E]:
    """
    Retries an async function returning Result[X, Exception].

    :param func: Async function returning Result[X, E]
    :param retries: Maximum number of retries
    :param delay: Initial delay in seconds
    :param backoff_factor: Multiplicative factor for exponential backoff
    :return: Result[X, E] (either success or failure after max retries)
    """
    attempt = 0
    while attempt < retries:
        result = await func(*fargs,**fkwargs)
        if result.is_ok:
            return result  # Success

        # If failure, extract the error
        error = result.unwrap_err()
        print(f"Attempt {attempt + 1}/{retries} failed with error: {error}, retrying in {delay}sec...")

        attempt += 1
        if attempt < retries:
            await asyncio.sleep(delay)
            delay *= backoff_factor  # Exponential backoff

    return result  # Return last failure after max retrie