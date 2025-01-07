from typing import AsyncContextManager, ContextManager
from contextlib import asynccontextmanager, contextmanager

try:
    import httpx
    from httpx import AsyncClient, Client, Response
except ImportError:
    pass


class HTTPClientManager:
    """Manages HTTP client lifecycle for both sync and async operations.

    This class provides context managers for both synchronous and asynchronous
    HTTP clients, ensuring proper resource cleanup.
    """

    @contextmanager
    def get_client(self) -> ContextManager[Client]:
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
    async def get_async_client(self) -> AsyncContextManager[AsyncClient]:
        """Get an asynchronous HTTP client within a context manager.

        Returns:
            AsyncContextManager[AsyncClient]: A context-managed httpx.AsyncClient instance.
        """
        client = AsyncClient()
        try:
            yield client
        finally:
            await client.aclose()

