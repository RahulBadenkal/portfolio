from typing import Optional
import httpx


async def httpx_wrapper(func, client: Optional[httpx.AsyncClient] = None, *args, **kwargs):
    if client is None:
        async with httpx.AsyncClient() as client:
            return await func(client, *args, **kwargs)
    else:
        return await func(client, *args, **kwargs)


def httpx_raise_for_status(response):
    class HTTPRequestError(Exception):
        def __init__(self, exc, body):
            self.body = body
            new_message = f"{exc}\nResponse body:\n{body}"
            super().__init__(new_message)

    try:
        response.raise_for_status()  # This will raise an exception for 4xx/5xx responses
    except httpx.HTTPStatusError as e:
        raise HTTPRequestError(e, e.response.text) from None
