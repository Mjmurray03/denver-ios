"""
Custom exceptions for GIS data acquisition.

This module defines a hierarchy of exceptions for handling various
error conditions that may occur during GIS API interactions.
"""

from typing import Optional


class AcquisitionError(Exception):
    """Base exception for all acquisition-related errors."""

    def __init__(self, message: str, cause: Optional[Exception] = None) -> None:
        """
        Initialize the acquisition error.

        Args:
            message: Human-readable error description.
            cause: The underlying exception that caused this error, if any.
        """
        super().__init__(message)
        self.message = message
        self.cause = cause

    def __str__(self) -> str:
        if self.cause:
            return f"{self.message} (caused by: {self.cause})"
        return self.message


class ConnectionError(AcquisitionError):
    """Raised when unable to establish connection to the GIS server."""

    pass


class TimeoutError(AcquisitionError):
    """Raised when a request times out."""

    def __init__(
        self,
        message: str,
        timeout_type: str = "unknown",
        cause: Optional[Exception] = None,
    ) -> None:
        """
        Initialize the timeout error.

        Args:
            message: Human-readable error description.
            timeout_type: Type of timeout (connect, read, write, pool).
            cause: The underlying exception that caused this error.
        """
        super().__init__(message, cause)
        self.timeout_type = timeout_type


class RateLimitError(AcquisitionError):
    """Raised when the API rate limit is exceeded."""

    def __init__(
        self,
        message: str,
        retry_after: Optional[float] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        """
        Initialize the rate limit error.

        Args:
            message: Human-readable error description.
            retry_after: Suggested wait time in seconds before retrying.
            cause: The underlying exception that caused this error.
        """
        super().__init__(message, cause)
        self.retry_after = retry_after


class ServerError(AcquisitionError):
    """Raised when the GIS server returns a 5xx error."""

    def __init__(
        self,
        message: str,
        status_code: int,
        cause: Optional[Exception] = None,
    ) -> None:
        """
        Initialize the server error.

        Args:
            message: Human-readable error description.
            status_code: HTTP status code returned by the server.
            cause: The underlying exception that caused this error.
        """
        super().__init__(message, cause)
        self.status_code = status_code


class AuthenticationError(AcquisitionError):
    """Raised when authentication fails (401/403)."""

    def __init__(
        self,
        message: str,
        status_code: int,
        cause: Optional[Exception] = None,
    ) -> None:
        """
        Initialize the authentication error.

        Args:
            message: Human-readable error description.
            status_code: HTTP status code (401 or 403).
            cause: The underlying exception that caused this error.
        """
        super().__init__(message, cause)
        self.status_code = status_code


class NotFoundError(AcquisitionError):
    """Raised when the requested resource is not found (404)."""

    def __init__(
        self,
        message: str,
        url: str,
        cause: Optional[Exception] = None,
    ) -> None:
        """
        Initialize the not found error.

        Args:
            message: Human-readable error description.
            url: The URL that was not found.
            cause: The underlying exception that caused this error.
        """
        super().__init__(message, cause)
        self.url = url


class InvalidResponseError(AcquisitionError):
    """Raised when the server returns an invalid or unparseable response."""

    def __init__(
        self,
        message: str,
        response_text: Optional[str] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        """
        Initialize the invalid response error.

        Args:
            message: Human-readable error description.
            response_text: The raw response text that couldn't be parsed.
            cause: The underlying exception that caused this error.
        """
        super().__init__(message, cause)
        self.response_text = response_text[:500] if response_text else None


class PaginationError(AcquisitionError):
    """Raised when pagination fails or returns inconsistent results."""

    def __init__(
        self,
        message: str,
        offset: int,
        expected_count: int,
        actual_count: int,
        cause: Optional[Exception] = None,
    ) -> None:
        """
        Initialize the pagination error.

        Args:
            message: Human-readable error description.
            offset: The offset at which pagination failed.
            expected_count: Expected number of records.
            actual_count: Actual number of records received.
            cause: The underlying exception that caused this error.
        """
        super().__init__(message, cause)
        self.offset = offset
        self.expected_count = expected_count
        self.actual_count = actual_count


class MaxRetriesExceededError(AcquisitionError):
    """Raised when maximum retry attempts have been exhausted."""

    def __init__(
        self,
        message: str,
        attempts: int,
        last_error: Optional[Exception] = None,
    ) -> None:
        """
        Initialize the max retries exceeded error.

        Args:
            message: Human-readable error description.
            attempts: Number of retry attempts made.
            last_error: The last error encountered before giving up.
        """
        super().__init__(message, last_error)
        self.attempts = attempts
        self.last_error = last_error


class GeometryError(AcquisitionError):
    """Raised when geometry data is invalid or cannot be processed."""

    def __init__(
        self,
        message: str,
        feature_id: Optional[str] = None,
        cause: Optional[Exception] = None,
    ) -> None:
        """
        Initialize the geometry error.

        Args:
            message: Human-readable error description.
            feature_id: Identifier of the feature with invalid geometry.
            cause: The underlying exception that caused this error.
        """
        super().__init__(message, cause)
        self.feature_id = feature_id
