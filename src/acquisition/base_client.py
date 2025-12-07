"""
Abstract base client for GIS data acquisition.

This module provides the AsyncGISClient base class that implements:
- Rate limiting via asyncio.Semaphore
- Retry logic with exponential backoff
- Proper httpx.AsyncClient lifecycle management
- Abstract methods for concrete implementations
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Optional

import httpx

from .exceptions import (
    AuthenticationError,
    ConnectionError,
    InvalidResponseError,
    MaxRetriesExceededError,
    NotFoundError,
    RateLimitError,
    ServerError,
    TimeoutError,
)
from .models import BoundingBox, GISClientConfig, LayerConfig

logger = logging.getLogger(__name__)


class AsyncGISClient(ABC):
    """
    Abstract base class for async GIS API clients.

    Provides common functionality for making rate-limited, retryable HTTP
    requests to GIS REST APIs. Subclasses must implement the abstract methods
    to handle specific API endpoints and response formats.

    Usage:
        async with MyGISClient(config) as client:
            data = await client.fetch_layer(layer_config)

    Attributes:
        config: The GISClientConfig instance with all settings.
        _client: The httpx.AsyncClient instance (created on context entry).
        _semaphore: Asyncio semaphore for rate limiting concurrent requests.
    """

    def __init__(self, config: GISClientConfig) -> None:
        """
        Initialize the GIS client with configuration.

        Args:
            config: GISClientConfig instance with all client settings.
        """
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._request_count = 0
        self._last_request_time: float = 0

    async def __aenter__(self) -> "AsyncGISClient":
        """
        Async context manager entry - creates the HTTP client.

        Returns:
            Self for use in async with statements.
        """
        await self._create_client()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Async context manager exit - closes the HTTP client.

        Args:
            exc_type: Exception type if an error occurred.
            exc_val: Exception value if an error occurred.
            exc_tb: Exception traceback if an error occurred.
        """
        await self._close_client()

    async def _create_client(self) -> None:
        """Create the httpx.AsyncClient with configured settings."""
        timeout = httpx.Timeout(
            connect=self.config.timeout.connect,
            read=self.config.timeout.read,
            write=self.config.timeout.write,
            pool=self.config.timeout.pool,
        )

        limits = httpx.Limits(
            max_connections=self.config.limits.max_connections,
            max_keepalive_connections=self.config.limits.max_keepalive_connections,
            keepalive_expiry=self.config.limits.keepalive_expiry,
        )

        self._client = httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            headers={
                "User-Agent": self.config.user_agent,
                "Accept": "application/json",
            },
            follow_redirects=True,
        )

        self._semaphore = asyncio.Semaphore(
            self.config.rate_limit.concurrent_requests
        )

        logger.info(
            "Created GIS client for %s (max %d concurrent, %d req/min)",
            self.config.base_url,
            self.config.rate_limit.concurrent_requests,
            self.config.rate_limit.requests_per_minute,
        )

    async def _close_client(self) -> None:
        """Close the httpx.AsyncClient and release resources."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info(
                "Closed GIS client (made %d requests)", self._request_count
            )

    def _ensure_client(self) -> httpx.AsyncClient:
        """
        Ensure the client is initialized and return it.

        Returns:
            The httpx.AsyncClient instance.

        Raises:
            RuntimeError: If client is not initialized (not in context manager).
        """
        if self._client is None:
            raise RuntimeError(
                "Client not initialized. Use 'async with' context manager."
            )
        return self._client

    async def _rate_limit_delay(self) -> None:
        """
        Apply rate limiting delay between requests.

        Ensures minimum interval between requests to stay within rate limits.
        """
        import time

        current_time = time.monotonic()
        elapsed = current_time - self._last_request_time

        if elapsed < self.config.rate_limit.min_request_interval:
            delay = self.config.rate_limit.min_request_interval - elapsed
            logger.debug("Rate limiting: sleeping %.3f seconds", delay)
            await asyncio.sleep(delay)

        self._last_request_time = time.monotonic()

    def _classify_http_error(
        self, error: httpx.HTTPStatusError, url: str
    ) -> Exception:
        """
        Convert httpx.HTTPStatusError to appropriate custom exception.

        Args:
            error: The httpx HTTPStatusError.
            url: The URL that was requested.

        Returns:
            Appropriate custom exception for the status code.
        """
        status = error.response.status_code

        if status == 429:
            retry_after = error.response.headers.get("Retry-After")
            retry_seconds = float(retry_after) if retry_after else None
            return RateLimitError(
                f"Rate limit exceeded for {url}",
                retry_after=retry_seconds,
                cause=error,
            )
        elif status in (401, 403):
            return AuthenticationError(
                f"Authentication failed for {url}: {status}",
                status_code=status,
                cause=error,
            )
        elif status == 404:
            return NotFoundError(
                f"Resource not found: {url}",
                url=url,
                cause=error,
            )
        elif status >= 500:
            return ServerError(
                f"Server error {status} for {url}",
                status_code=status,
                cause=error,
            )
        else:
            # For other 4xx errors, wrap in generic error
            return InvalidResponseError(
                f"HTTP {status} error for {url}",
                response_text=error.response.text,
                cause=error,
            )

    def _classify_transport_error(
        self, error: httpx.TransportError, url: str
    ) -> Exception:
        """
        Convert httpx transport errors to appropriate custom exceptions.

        Args:
            error: The httpx TransportError.
            url: The URL that was requested.

        Returns:
            Appropriate custom exception for the error type.
        """
        if isinstance(error, httpx.TimeoutException):
            timeout_type = "unknown"
            if isinstance(error, httpx.ConnectTimeout):
                timeout_type = "connect"
            elif isinstance(error, httpx.ReadTimeout):
                timeout_type = "read"
            elif isinstance(error, httpx.WriteTimeout):
                timeout_type = "write"
            elif isinstance(error, httpx.PoolTimeout):
                timeout_type = "pool"

            return TimeoutError(
                f"Request to {url} timed out ({timeout_type})",
                timeout_type=timeout_type,
                cause=error,
            )
        elif isinstance(error, httpx.ConnectError):
            return ConnectionError(
                f"Failed to connect to {url}",
                cause=error,
            )
        else:
            return ConnectionError(
                f"Transport error for {url}: {error}",
                cause=error,
            )

    def _is_retryable_error(self, error: Exception) -> bool:
        """
        Determine if an error is retryable.

        Args:
            error: The exception to check.

        Returns:
            True if the request should be retried, False otherwise.
        """
        if isinstance(error, (TimeoutError, ConnectionError, RateLimitError)):
            return True
        if isinstance(error, ServerError) and error.status_code in (
            500, 502, 503, 504
        ):
            return True
        return False

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Make an HTTP request with retry logic and rate limiting.

        Args:
            method: HTTP method (GET, POST, etc.).
            url: The URL to request.
            **kwargs: Additional arguments passed to httpx.request().

        Returns:
            The httpx.Response object.

        Raises:
            MaxRetriesExceededError: If all retry attempts fail.
            AuthenticationError: If authentication fails (not retried).
            NotFoundError: If resource is not found (not retried).
            InvalidResponseError: For non-retryable HTTP errors.
        """
        client = self._ensure_client()
        last_error: Optional[Exception] = None

        for attempt in range(self.config.retry.max_retries + 1):
            try:
                # Acquire semaphore for concurrent request limiting
                async with self._semaphore:  # type: ignore
                    await self._rate_limit_delay()

                    logger.debug(
                        "Request attempt %d/%d: %s %s",
                        attempt + 1,
                        self.config.retry.max_retries + 1,
                        method,
                        url,
                    )

                    response = await client.request(method, url, **kwargs)
                    self._request_count += 1

                    response.raise_for_status()
                    return response

            except httpx.HTTPStatusError as e:
                error = self._classify_http_error(e, url)

                if not self._is_retryable_error(error):
                    raise error

                last_error = error
                logger.warning(
                    "Retryable HTTP error on attempt %d: %s",
                    attempt + 1,
                    error,
                )

            except httpx.TransportError as e:
                error = self._classify_transport_error(e, url)
                last_error = error
                logger.warning(
                    "Transport error on attempt %d: %s",
                    attempt + 1,
                    error,
                )

            # If we have more attempts, calculate delay and retry
            if attempt < self.config.retry.max_retries:
                # For rate limit errors, use retry_after if available
                if isinstance(last_error, RateLimitError) and last_error.retry_after:
                    delay = last_error.retry_after
                else:
                    delay = self.config.retry.calculate_delay(attempt)

                logger.info(
                    "Retrying in %.2f seconds (attempt %d/%d)",
                    delay,
                    attempt + 2,
                    self.config.retry.max_retries + 1,
                )
                await asyncio.sleep(delay)

        # All retries exhausted
        raise MaxRetriesExceededError(
            f"Max retries ({self.config.retry.max_retries}) exceeded for {url}",
            attempts=self.config.retry.max_retries + 1,
            last_error=last_error,
        )

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        """
        Make a GET request with retry logic.

        Args:
            url: The URL to request.
            **kwargs: Additional arguments passed to httpx.request().

        Returns:
            The httpx.Response object.
        """
        return await self._request_with_retry("GET", url, **kwargs)

    async def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        """
        Make a GET request and parse JSON response.

        Args:
            url: The URL to request.
            **kwargs: Additional arguments passed to httpx.request().

        Returns:
            Parsed JSON response as a dictionary.

        Raises:
            InvalidResponseError: If response is not valid JSON.
        """
        response = await self.get(url, **kwargs)

        try:
            return response.json()
        except Exception as e:
            raise InvalidResponseError(
                f"Failed to parse JSON from {url}",
                response_text=response.text,
                cause=e,
            )

    async def fetch_record_count(
        self,
        layer_url: str,
        where: str = "1=1",
        geometry: Optional[BoundingBox] = None,
    ) -> int:
        """
        Fetch the total record count for a layer query.

        Args:
            layer_url: The layer query endpoint URL.
            where: SQL WHERE clause for filtering.
            geometry: Optional bounding box to filter by geometry.

        Returns:
            Total number of records matching the query.
        """
        params: dict[str, Any] = {
            "where": where,
            "returnCountOnly": "true",
            "f": "json",
        }

        if geometry:
            params["geometry"] = geometry.to_esri_envelope()
            params["geometryType"] = "esriGeometryEnvelope"
            params["spatialRel"] = "esriSpatialRelIntersects"
            params["inSR"] = "4326"

        url = f"{layer_url}/query"
        data = await self.get_json(url, params=params)

        count = data.get("count", 0)
        logger.info("Layer %s has %d records matching query", layer_url, count)
        return count

    async def fetch_page(
        self,
        layer_url: str,
        offset: int,
        where: str = "1=1",
        out_fields: str = "*",
        geometry: Optional[BoundingBox] = None,
        return_geometry: bool = True,
    ) -> dict[str, Any]:
        """
        Fetch a single page of records from a layer.

        Args:
            layer_url: The layer query endpoint URL.
            offset: The result offset (starting record index).
            where: SQL WHERE clause for filtering.
            out_fields: Comma-separated list of fields to return.
            geometry: Optional bounding box to filter by geometry.
            return_geometry: Whether to include geometry in response.

        Returns:
            GeoJSON FeatureCollection or ESRI JSON response.
        """
        params: dict[str, Any] = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": str(return_geometry).lower(),
            "resultOffset": offset,
            "resultRecordCount": self.config.pagination.page_size,
            "f": "geojson",
        }

        if geometry:
            params["geometry"] = geometry.to_esri_envelope()
            params["geometryType"] = "esriGeometryEnvelope"
            params["spatialRel"] = "esriSpatialRelIntersects"
            params["inSR"] = "4326"
            params["outSR"] = "4326"

        url = f"{layer_url}/query"
        data = await self.get_json(url, params=params)

        feature_count = len(data.get("features", []))
        logger.debug(
            "Fetched page at offset %d: %d features",
            offset,
            feature_count,
        )

        return data

    async def fetch_all_pages(
        self,
        layer_url: str,
        where: str = "1=1",
        out_fields: str = "*",
        geometry: Optional[BoundingBox] = None,
        return_geometry: bool = True,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Fetch all pages of records from a layer as an async generator.

        Yields pages one at a time to allow processing while fetching.

        Args:
            layer_url: The layer query endpoint URL.
            where: SQL WHERE clause for filtering.
            out_fields: Comma-separated list of fields to return.
            geometry: Optional bounding box to filter by geometry.
            return_geometry: Whether to include geometry in response.

        Yields:
            GeoJSON FeatureCollection for each page.
        """
        # First, get total count
        total_count = await self.fetch_record_count(layer_url, where, geometry)

        if total_count == 0:
            logger.info("No records found for layer %s", layer_url)
            return

        # Apply max_total_records limit if configured
        if self.config.pagination.max_total_records:
            total_count = min(total_count, self.config.pagination.max_total_records)

        # Calculate number of pages
        page_size = self.config.pagination.page_size
        num_pages = (total_count + page_size - 1) // page_size

        logger.info(
            "Fetching %d records in %d pages (page size: %d)",
            total_count,
            num_pages,
            page_size,
        )

        # Fetch pages sequentially to respect rate limits
        fetched_count = 0
        for page_num in range(num_pages):
            offset = page_num * page_size

            page_data = await self.fetch_page(
                layer_url=layer_url,
                offset=offset,
                where=where,
                out_fields=out_fields,
                geometry=geometry,
                return_geometry=return_geometry,
            )

            features = page_data.get("features", [])
            fetched_count += len(features)

            logger.info(
                "Progress: %d/%d records (%.1f%%)",
                fetched_count,
                total_count,
                100 * fetched_count / total_count,
            )

            yield page_data

            # Check for exceededTransferLimit flag
            if page_data.get("exceededTransferLimit", False):
                logger.warning(
                    "Transfer limit exceeded at offset %d, continuing...",
                    offset,
                )

    @abstractmethod
    async def fetch_layer(
        self,
        layer_config: LayerConfig,
        geometry: Optional[BoundingBox] = None,
    ) -> dict[str, Any]:
        """
        Fetch all data from a specific layer.

        Subclasses must implement this method to handle layer-specific
        logic such as field mappings and response processing.

        Args:
            layer_config: Configuration for the layer to fetch.
            geometry: Optional bounding box to filter by geometry.

        Returns:
            Combined GeoJSON FeatureCollection with all features.
        """
        pass

    @abstractmethod
    def get_layer_url(self, layer_config: LayerConfig) -> str:
        """
        Get the full URL for a specific layer.

        Subclasses must implement this to construct the correct URL
        for their specific GIS service.

        Args:
            layer_config: Configuration for the layer.

        Returns:
            Full URL to the layer endpoint.
        """
        pass
