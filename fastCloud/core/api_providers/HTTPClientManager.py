from typing import Generator, AsyncGenerator
from contextlib import asynccontextmanager, contextmanager

try:
    from httpx import AsyncClient, Client
except ImportError:
    pass


class HTTPClientManager:
    """Manages HTTP client lifecycle for both sync and async operations.

    This class provides context managers for both synchronous and asynchronous
    HTTP clients, ensuring proper resource cleanup.
    """

    @contextmanager
    def get_client(self) -> Generator[Client, None, None]:
        """Get a synchronous HTTP client within a context manager.

        Returns:
            ContextManager[Client]: A context-managed httpx.Client instance.
        """
        client = Client()
        try:
            yield client
        finally:
            client.close()

    @asynccontextmanager
    async def get_async_client(self) -> AsyncGenerator[AsyncClient, None]:
        """Get an asynchronous HTTP client within a context manager.

        Returns:
            AsyncContextManager[AsyncClient]: A context-managed httpx.AsyncClient instance.
        """
        client = AsyncClient()
        try:
            yield client
        finally:
            await client.aclose()
