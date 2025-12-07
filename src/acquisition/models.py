"""
Pydantic models for GIS data acquisition configuration and data structures.

This module defines configuration models for HTTP clients, rate limiting,
retry behavior, and geographic boundaries used in the acquisition process.
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class RetryConfig(BaseModel):
    """Configuration for retry behavior with exponential backoff."""

    max_retries: int = Field(
        default=5,
        ge=0,
        le=10,
        description="Maximum number of retry attempts",
    )
    base_delay: float = Field(
        default=1.0,
        gt=0,
        le=10.0,
        description="Base delay in seconds for exponential backoff",
    )
    max_delay: float = Field(
        default=30.0,
        gt=0,
        le=300.0,
        description="Maximum delay in seconds between retries",
    )
    jitter_factor: float = Field(
        default=0.5,
        ge=0,
        le=1.0,
        description="Random jitter factor (0-1) to add to delays",
    )

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate the delay for a given retry attempt.

        Uses exponential backoff with jitter: delay = min(base * 2^attempt + jitter, max)

        Args:
            attempt: The current retry attempt number (0-indexed).

        Returns:
            The delay in seconds before the next retry.
        """
        import random

        exponential_delay = self.base_delay * (2**attempt)
        jitter = random.uniform(0, self.jitter_factor)
        return min(exponential_delay + jitter, self.max_delay)


class RateLimitConfig(BaseModel):
    """Configuration for API rate limiting."""

    requests_per_minute: int = Field(
        default=120,
        gt=0,
        le=1000,
        description="Maximum requests per minute",
    )
    concurrent_requests: int = Field(
        default=2,
        gt=0,
        le=10,
        description="Maximum concurrent requests",
    )
    min_request_interval: float = Field(
        default=0.5,
        ge=0,
        le=10.0,
        description="Minimum interval between requests in seconds",
    )

    @field_validator("min_request_interval", mode="before")
    @classmethod
    def calculate_interval(cls, v: float, info) -> float:
        """Calculate minimum interval if not explicitly set."""
        if v is None and "requests_per_minute" in info.data:
            return 60.0 / info.data["requests_per_minute"]
        return v


class TimeoutConfig(BaseModel):
    """Configuration for HTTP request timeouts."""

    connect: float = Field(
        default=10.0,
        gt=0,
        le=60.0,
        description="Timeout for establishing connection in seconds",
    )
    read: float = Field(
        default=60.0,
        gt=0,
        le=300.0,
        description="Timeout for reading response in seconds",
    )
    write: float = Field(
        default=10.0,
        gt=0,
        le=60.0,
        description="Timeout for writing request in seconds",
    )
    pool: float = Field(
        default=30.0,
        gt=0,
        le=60.0,
        description="Timeout for acquiring connection from pool in seconds",
    )


class ConnectionLimits(BaseModel):
    """Configuration for HTTP connection pool limits."""

    max_connections: int = Field(
        default=10,
        gt=0,
        le=100,
        description="Maximum total connections",
    )
    max_keepalive_connections: int = Field(
        default=5,
        gt=0,
        le=50,
        description="Maximum keepalive connections",
    )
    keepalive_expiry: float = Field(
        default=30.0,
        gt=0,
        le=300.0,
        description="Keepalive connection expiry in seconds",
    )


class PaginationConfig(BaseModel):
    """Configuration for paginated API requests."""

    page_size: int = Field(
        default=1000,
        gt=0,
        le=5000,
        description="Number of records per page",
    )
    max_total_records: Optional[int] = Field(
        default=None,
        gt=0,
        description="Maximum total records to fetch (None for unlimited)",
    )


class BoundingBox(BaseModel):
    """Geographic bounding box defined by corner coordinates."""

    min_x: float = Field(..., ge=-180, le=180, description="Minimum longitude")
    min_y: float = Field(..., ge=-90, le=90, description="Minimum latitude")
    max_x: float = Field(..., ge=-180, le=180, description="Maximum longitude")
    max_y: float = Field(..., ge=-90, le=90, description="Maximum latitude")

    @field_validator("max_x")
    @classmethod
    def validate_x_range(cls, v: float, info) -> float:
        """Ensure max_x is greater than min_x."""
        if "min_x" in info.data and v <= info.data["min_x"]:
            raise ValueError("max_x must be greater than min_x")
        return v

    @field_validator("max_y")
    @classmethod
    def validate_y_range(cls, v: float, info) -> float:
        """Ensure max_y is greater than min_y."""
        if "min_y" in info.data and v <= info.data["min_y"]:
            raise ValueError("max_y must be greater than min_y")
        return v

    def to_esri_envelope(self) -> str:
        """
        Convert to ESRI envelope format for API queries.

        Returns:
            String in format "minX,minY,maxX,maxY"
        """
        return f"{self.min_x},{self.min_y},{self.max_x},{self.max_y}"

    def to_wkt(self) -> str:
        """
        Convert to WKT polygon format.

        Returns:
            WKT POLYGON string.
        """
        return (
            f"POLYGON(("
            f"{self.min_x} {self.min_y}, "
            f"{self.max_x} {self.min_y}, "
            f"{self.max_x} {self.max_y}, "
            f"{self.min_x} {self.max_y}, "
            f"{self.min_x} {self.min_y}))"
        )

    @classmethod
    def from_center_radius(
        cls, center_lat: float, center_lon: float, radius_km: float
    ) -> "BoundingBox":
        """
        Create a bounding box from a center point and radius.

        Note: This is an approximation that works well for small areas.
        For more accuracy at larger scales, use proper geodetic calculations.

        Args:
            center_lat: Center latitude in degrees.
            center_lon: Center longitude in degrees.
            radius_km: Radius in kilometers.

        Returns:
            BoundingBox instance.
        """
        import math

        # Approximate degrees per km at the given latitude
        # 1 degree latitude ~ 111 km
        # 1 degree longitude ~ 111 * cos(lat) km
        lat_delta = radius_km / 111.0
        lon_delta = radius_km / (111.0 * math.cos(math.radians(center_lat)))

        return cls(
            min_x=center_lon - lon_delta,
            min_y=center_lat - lat_delta,
            max_x=center_lon + lon_delta,
            max_y=center_lat + lat_delta,
        )


class TargetArea(BaseModel):
    """
    Target geographic area for property sourcing.

    Default values are for Commerce City industrial area in Adams County.
    """

    center_latitude: float = Field(
        default=39.82026,
        ge=-90,
        le=90,
        description="Center point latitude",
    )
    center_longitude: float = Field(
        default=-104.90811,
        ge=-180,
        le=180,
        description="Center point longitude",
    )
    radius_km: float = Field(
        default=3.0,
        gt=0,
        le=50.0,
        description="Search radius in kilometers",
    )
    crs: str = Field(
        default="EPSG:4326",
        description="Coordinate reference system",
    )

    def get_bounding_box(self) -> BoundingBox:
        """
        Get the bounding box for this target area.

        Returns:
            BoundingBox instance covering the target area.
        """
        return BoundingBox.from_center_radius(
            self.center_latitude, self.center_longitude, self.radius_km
        )


class LayerType(str, Enum):
    """Types of GIS layers available from Adams County."""

    PARCELS = "parcels"
    ZONING = "zoning"
    ASSESSMENTS = "assessments"
    BUILDINGS = "buildings"


class LayerConfig(BaseModel):
    """Configuration for a specific GIS layer."""

    layer_id: int = Field(..., ge=0, description="Layer ID in the feature service")
    name: str = Field(..., min_length=1, description="Human-readable layer name")
    layer_type: LayerType = Field(..., description="Type of layer")
    out_fields: list[str] = Field(
        default=["*"],
        min_length=1,
        description="Fields to retrieve",
    )
    where_clause: str = Field(
        default="1=1",
        description="SQL WHERE clause for filtering",
    )


class GISClientConfig(BaseModel):
    """
    Complete configuration for a GIS API client.

    Aggregates all configuration options for timeouts, rate limiting,
    retries, pagination, and connection management.
    """

    base_url: str = Field(
        ...,
        min_length=1,
        description="Base URL for the GIS REST API",
    )
    timeout: TimeoutConfig = Field(
        default_factory=TimeoutConfig,
        description="Timeout configuration",
    )
    limits: ConnectionLimits = Field(
        default_factory=ConnectionLimits,
        description="Connection pool limits",
    )
    retry: RetryConfig = Field(
        default_factory=RetryConfig,
        description="Retry configuration",
    )
    rate_limit: RateLimitConfig = Field(
        default_factory=RateLimitConfig,
        description="Rate limiting configuration",
    )
    pagination: PaginationConfig = Field(
        default_factory=PaginationConfig,
        description="Pagination configuration",
    )
    target_area: TargetArea = Field(
        default_factory=TargetArea,
        description="Target geographic area",
    )
    user_agent: str = Field(
        default="DenverIOS-PropertySourcing/1.0",
        description="User-Agent header for requests",
    )


# Pre-configured defaults for Adams County
ADAMS_COUNTY_CONFIG = GISClientConfig(
    base_url="https://gis.adcogov.org/arcgis/rest/services",
    timeout=TimeoutConfig(
        connect=10.0,
        read=60.0,
        write=10.0,
        pool=30.0,
    ),
    limits=ConnectionLimits(
        max_connections=10,
        max_keepalive_connections=5,
        keepalive_expiry=30.0,
    ),
    retry=RetryConfig(
        max_retries=5,
        base_delay=1.0,
        max_delay=30.0,
        jitter_factor=0.5,
    ),
    rate_limit=RateLimitConfig(
        requests_per_minute=120,
        concurrent_requests=2,
        min_request_interval=0.5,
    ),
    pagination=PaginationConfig(
        page_size=1000,
        max_total_records=None,
    ),
    target_area=TargetArea(
        center_latitude=39.82026,
        center_longitude=-104.90811,
        radius_km=3.0,
    ),
)
