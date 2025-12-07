"""
GIS Data Acquisition Module for Denver IOS Property Sourcing.

This module provides data loaders for fetching property data from
local files (File Geodatabase, GeoPackage) and GIS REST APIs.

Primary Usage (File-based loading - recommended):
    from src.acquisition import AdamsCountyFileLoader

    loader = AdamsCountyFileLoader()
    parcels = loader.load_parcels()
    buildings = loader.load_building_footprints()

Legacy API Client (deprecated):
    The AdamsCountyClient REST API client is deprecated.
    Import from adams_county_api if needed for reference.
"""

from .exceptions import (
    AcquisitionError,
    AuthenticationError,
    ConnectionError,
    GeometryError,
    InvalidResponseError,
    MaxRetriesExceededError,
    NotFoundError,
    PaginationError,
    RateLimitError,
    ServerError,
    TimeoutError,
)
from .models import (
    ADAMS_COUNTY_CONFIG,
    BoundingBox,
    ConnectionLimits,
    GISClientConfig,
    LayerConfig,
    LayerType,
    PaginationConfig,
    RateLimitConfig,
    RetryConfig,
    TargetArea,
    TimeoutConfig,
)
from .file_loader import AdamsCountyFileLoader

# Legacy imports (deprecated) - import directly from adams_county_api if needed
# from .adams_county_api import AdamsCountyClient

__all__ = [
    # Exceptions
    "AcquisitionError",
    "AuthenticationError",
    "ConnectionError",
    "GeometryError",
    "InvalidResponseError",
    "MaxRetriesExceededError",
    "NotFoundError",
    "PaginationError",
    "RateLimitError",
    "ServerError",
    "TimeoutError",
    # Models
    "BoundingBox",
    "ConnectionLimits",
    "GISClientConfig",
    "LayerConfig",
    "LayerType",
    "PaginationConfig",
    "RateLimitConfig",
    "RetryConfig",
    "TargetArea",
    "TimeoutConfig",
    # Pre-configured
    "ADAMS_COUNTY_CONFIG",
    # File Loader (primary)
    "AdamsCountyFileLoader",
]
