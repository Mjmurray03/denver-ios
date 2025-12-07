"""
Adams County GIS data acquisition client.

This module provides the AdamsCountyClient for fetching property data
from the Adams County, Colorado GIS REST API. It handles parcel data,
zoning information, and property assessments.

Adams County GIS Portal: https://gis.adcogov.org/arcgis/rest/services/
"""

import asyncio
import logging
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import shape

from .base_client import AsyncGISClient
from .exceptions import InvalidResponseError
from .models import (
    ADAMS_COUNTY_CONFIG,
    BoundingBox,
    GISClientConfig,
    LayerConfig,
    LayerType,
)

logger = logging.getLogger(__name__)


# Known Adams County Feature Service paths
# These may need adjustment based on actual API exploration
ADAMS_COUNTY_SERVICES = {
    "parcels": "Cadastral/Parcels/FeatureServer",
    "zoning": "Planning/Zoning/FeatureServer",
    "assessments": "Assessor/PropertyAssessment/FeatureServer",
}

# Field name mappings from Adams County GIS to standardized names
# Keys are our standardized names, values are potential ArcGIS field names
# Multiple options provided since exact names may vary
PARCEL_FIELD_MAPPINGS = {
    # Parcel identification
    "parcel_id": ["PARCEL_ID", "PARCELID", "PARCEL", "PIN", "APN"],
    "apn": ["APN", "ASSESSOR_PARCEL_NUMBER", "PARCEL_NUMBER", "ACCOUNT"],

    # Address information
    "address": ["ADDRESS", "SITUS_ADDRESS", "SITE_ADDRESS", "SITUS", "SITEADDRESS"],
    "city": ["CITY", "SITUS_CITY", "SITE_CITY"],
    "state": ["STATE", "SITUS_STATE"],
    "zip_code": ["ZIP", "ZIP_CODE", "ZIPCODE", "SITUS_ZIP", "POSTAL_CODE"],

    # Property characteristics
    "acres": ["ACRES", "ACREAGE", "LAND_AREA", "GISACRES", "CALCACRES", "SHAPE_AREA"],
    "land_use_code": ["LAND_USE", "LANDUSE", "LAND_USE_CODE", "USE_CODE", "PROPCLASS"],
    "zoning": ["ZONING", "ZONE", "ZONE_CODE", "ZONING_CODE"],
    "legal_description": ["LEGAL", "LEGAL_DESC", "LEGALDESC", "LEGAL_DESCRIPTION"],

    # Valuation
    "land_value": ["LAND_VALUE", "LANDVALUE", "LAND_ACTUAL", "ASSESSED_LAND"],
    "improvement_value": ["IMPR_VALUE", "IMPROVEMENT_VALUE", "IMPVALUE", "BLDG_VALUE"],
    "total_value": ["TOTAL_VALUE", "TOTALVALUE", "ASSESSED_VALUE", "MARKET_VALUE"],

    # Owner information
    "owner_name": ["OWNER", "OWNER_NAME", "OWNERNAME", "OWNER1"],
    "owner_address": ["OWNER_ADDRESS", "OWNERADDR", "MAIL_ADDRESS", "MAILING_ADDRESS"],
    "owner_city": ["OWNER_CITY", "OWNERCITY", "MAIL_CITY"],
    "owner_state": ["OWNER_STATE", "OWNERSTATE", "MAIL_STATE"],
    "owner_zip": ["OWNER_ZIP", "OWNERZIP", "MAIL_ZIP"],
}


class AdamsCountyClient(AsyncGISClient):
    """
    Client for fetching GIS data from Adams County, Colorado.

    This client provides methods to fetch parcel data, zoning information,
    and property assessments from the Adams County GIS REST API.

    Usage:
        async with AdamsCountyClient() as client:
            # Discover available fields
            fields = await client.get_layer_fields("Cadastral/Parcels/FeatureServer/0")

            # Fetch parcels within a boundary
            parcels = await client.fetch_parcels(boundary)

            # Or get as GeoDataFrame
            gdf = await client.fetch_all_parcels(boundary)

    Attributes:
        config: GISClientConfig with Adams County specific settings.
        field_cache: Cache of discovered field names per layer.
    """

    def __init__(self, config: Optional[GISClientConfig] = None) -> None:
        """
        Initialize the Adams County GIS client.

        Args:
            config: Optional GISClientConfig. If not provided, uses
                    ADAMS_COUNTY_CONFIG defaults.
        """
        super().__init__(config or ADAMS_COUNTY_CONFIG)
        self._field_cache: dict[str, list[dict[str, Any]]] = {}

    def get_layer_url(self, layer_config: LayerConfig) -> str:
        """
        Construct the full URL for a layer.

        Args:
            layer_config: Configuration for the layer.

        Returns:
            Full URL to the layer endpoint.
        """
        # Use the service path from known services or construct from layer_id
        service_path = ADAMS_COUNTY_SERVICES.get(
            layer_config.layer_type.value,
            f"Unknown/{layer_config.name}/FeatureServer",
        )
        return f"{self.config.base_url}/{service_path}/{layer_config.layer_id}"

    def get_service_url(self, service_path: str) -> str:
        """
        Construct URL for a service path.

        Args:
            service_path: Relative path to the service (e.g., "Cadastral/Parcels/FeatureServer/0")

        Returns:
            Full URL to the service endpoint.
        """
        return f"{self.config.base_url}/{service_path}"

    async def get_service_info(self, service_path: str) -> dict[str, Any]:
        """
        Get metadata about a feature service or layer.

        Args:
            service_path: Relative path to the service.

        Returns:
            Service metadata including name, description, and capabilities.
        """
        url = f"{self.get_service_url(service_path)}?f=json"
        return await self.get_json(url)

    async def get_layer_fields(self, layer_path: str) -> list[dict[str, Any]]:
        """
        Get the field definitions for a layer.

        This is useful for discovering the actual field names used by
        the Adams County GIS, which may differ from expected names.

        Args:
            layer_path: Full path to the layer (e.g., "Cadastral/Parcels/FeatureServer/0")

        Returns:
            List of field definitions with name, type, alias, etc.
        """
        # Check cache first
        if layer_path in self._field_cache:
            return self._field_cache[layer_path]

        info = await self.get_service_info(layer_path)
        fields = info.get("fields", [])

        # Cache the results
        self._field_cache[layer_path] = fields

        logger.info(
            "Discovered %d fields for layer %s",
            len(fields),
            layer_path,
        )

        return fields

    async def list_available_services(self) -> list[dict[str, Any]]:
        """
        List all available services from the Adams County GIS.

        Returns:
            List of service metadata dictionaries.
        """
        url = f"{self.config.base_url}?f=json"
        data = await self.get_json(url)
        return data.get("services", [])

    async def discover_layer_by_name(
        self, name_pattern: str
    ) -> list[dict[str, Any]]:
        """
        Search for layers matching a name pattern.

        Useful for finding the correct service path when the exact
        endpoint is unknown.

        Args:
            name_pattern: Partial name to search for (case-insensitive).

        Returns:
            List of matching service metadata.
        """
        services = await self.list_available_services()
        pattern = name_pattern.lower()

        matches = [
            svc for svc in services
            if pattern in svc.get("name", "").lower()
        ]

        logger.info(
            "Found %d services matching '%s'",
            len(matches),
            name_pattern,
        )

        return matches

    def _map_field_name(
        self,
        standardized_name: str,
        available_fields: list[str],
    ) -> Optional[str]:
        """
        Find the actual field name for a standardized name.

        Args:
            standardized_name: Our standardized field name (e.g., "parcel_id").
            available_fields: List of actual field names from the API.

        Returns:
            The matching field name, or None if not found.
        """
        possible_names = PARCEL_FIELD_MAPPINGS.get(standardized_name, [])
        available_upper = {f.upper(): f for f in available_fields}

        for name in possible_names:
            if name.upper() in available_upper:
                return available_upper[name.upper()]

        return None

    def _create_field_mapping(
        self, available_fields: list[str]
    ) -> dict[str, str]:
        """
        Create a mapping from actual field names to standardized names.

        Args:
            available_fields: List of field names from the API.

        Returns:
            Dictionary mapping actual names to standardized names.
        """
        mapping = {}

        for std_name in PARCEL_FIELD_MAPPINGS:
            actual_name = self._map_field_name(std_name, available_fields)
            if actual_name:
                mapping[actual_name] = std_name

        logger.debug(
            "Created field mapping: %d of %d standardized fields mapped",
            len(mapping),
            len(PARCEL_FIELD_MAPPINGS),
        )

        return mapping

    async def fetch_layer(
        self,
        layer_config: LayerConfig,
        geometry: Optional[BoundingBox] = None,
    ) -> dict[str, Any]:
        """
        Fetch all data from a specific layer.

        Args:
            layer_config: Configuration for the layer to fetch.
            geometry: Optional bounding box to filter by geometry.

        Returns:
            Combined GeoJSON FeatureCollection with all features.
        """
        layer_url = self.get_layer_url(layer_config)

        # Collect all features from all pages
        all_features: list[dict[str, Any]] = []

        async for page in self.fetch_all_pages(
            layer_url=layer_url,
            where=layer_config.where_clause,
            out_fields=",".join(layer_config.out_fields),
            geometry=geometry,
            return_geometry=True,
        ):
            features = page.get("features", [])
            all_features.extend(features)

        # Construct combined GeoJSON
        result = {
            "type": "FeatureCollection",
            "features": all_features,
        }

        logger.info(
            "Fetched %d total features from layer %s",
            len(all_features),
            layer_config.name,
        )

        return result

    async def fetch_parcels(
        self,
        boundary: Optional[BoundingBox] = None,
        layer_path: str = "Cadastral/Parcels/FeatureServer/0",
        where: str = "1=1",
        max_records: Optional[int] = None,
    ) -> dict[str, Any]:
        """
        Fetch parcel data from Adams County GIS.

        Args:
            boundary: Optional bounding box to filter parcels.
                      If not provided, uses the target area from config.
            layer_path: Path to the parcels layer.
            where: SQL WHERE clause for additional filtering.
            max_records: Maximum records to fetch (for testing).

        Returns:
            GeoJSON FeatureCollection with parcel features.
        """
        if boundary is None:
            boundary = self.config.target_area.get_bounding_box()

        layer_url = self.get_service_url(layer_path)

        # If max_records is set, override pagination config temporarily
        original_max = self.config.pagination.max_total_records
        if max_records is not None:
            self.config.pagination.max_total_records = max_records

        try:
            all_features: list[dict[str, Any]] = []

            async for page in self.fetch_all_pages(
                layer_url=layer_url,
                where=where,
                out_fields="*",
                geometry=boundary,
                return_geometry=True,
            ):
                features = page.get("features", [])
                all_features.extend(features)

                # Early exit if we've hit max_records
                if max_records and len(all_features) >= max_records:
                    all_features = all_features[:max_records]
                    break

            result = {
                "type": "FeatureCollection",
                "features": all_features,
            }

            logger.info("Fetched %d parcels", len(all_features))
            return result

        finally:
            # Restore original pagination setting
            self.config.pagination.max_total_records = original_max

    async def fetch_all_parcels(
        self,
        boundary: Optional[BoundingBox] = None,
        layer_path: str = "Cadastral/Parcels/FeatureServer/0",
        standardize_fields: bool = True,
    ) -> gpd.GeoDataFrame:
        """
        Fetch all parcels and return as a GeoDataFrame.

        This is a convenience method that fetches parcel data and
        converts it to a GeoDataFrame with optional field standardization.

        Args:
            boundary: Optional bounding box to filter parcels.
            layer_path: Path to the parcels layer.
            standardize_fields: If True, rename fields to standardized names.

        Returns:
            GeoDataFrame with parcel data.
        """
        # Fetch the GeoJSON data
        geojson = await self.fetch_parcels(boundary, layer_path)

        features = geojson.get("features", [])
        if not features:
            logger.warning("No parcels found in the specified area")
            return gpd.GeoDataFrame()

        # Convert to GeoDataFrame
        try:
            gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
        except Exception as e:
            raise InvalidResponseError(
                "Failed to convert GeoJSON to GeoDataFrame",
                cause=e,
            )

        logger.info(
            "Created GeoDataFrame with %d parcels and %d columns",
            len(gdf),
            len(gdf.columns),
        )

        # Optionally standardize field names
        if standardize_fields:
            field_mapping = self._create_field_mapping(list(gdf.columns))
            gdf = gdf.rename(columns=field_mapping)
            logger.debug("Standardized %d field names", len(field_mapping))

        return gdf

    async def fetch_sample_parcels(
        self,
        n: int = 10,
        boundary: Optional[BoundingBox] = None,
    ) -> gpd.GeoDataFrame:
        """
        Fetch a small sample of parcels for testing.

        Args:
            n: Number of parcels to fetch.
            boundary: Optional bounding box to filter parcels.

        Returns:
            GeoDataFrame with sample parcel data.
        """
        geojson = await self.fetch_parcels(
            boundary=boundary,
            max_records=n,
        )

        features = geojson.get("features", [])
        if not features:
            return gpd.GeoDataFrame()

        return gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")

    async def print_layer_schema(self, layer_path: str) -> None:
        """
        Print the schema (field definitions) for a layer.

        Useful for debugging and discovering field names.

        Args:
            layer_path: Path to the layer.
        """
        fields = await self.get_layer_fields(layer_path)

        print(f"\n{'='*60}")
        print(f"Layer Schema: {layer_path}")
        print(f"{'='*60}")
        print(f"{'Field Name':<30} {'Type':<15} {'Alias'}")
        print(f"{'-'*30} {'-'*15} {'-'*30}")

        for field in fields:
            name = field.get("name", "")
            ftype = field.get("type", "").replace("esriFieldType", "")
            alias = field.get("alias", "")
            print(f"{name:<30} {ftype:<15} {alias}")

        print(f"{'='*60}\n")


async def main() -> None:
    """
    Test function for Adams County GIS client.

    Demonstrates basic usage including service discovery,
    field schema inspection, and parcel fetching.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("\n" + "="*60)
    print("Adams County GIS Client Test")
    print("="*60)

    async with AdamsCountyClient() as client:
        # Step 1: List available services
        print("\n[1] Discovering available services...")
        try:
            services = await client.list_available_services()
            print(f"Found {len(services)} services:")
            for svc in services[:10]:  # Show first 10
                print(f"  - {svc.get('name')} ({svc.get('type')})")
            if len(services) > 10:
                print(f"  ... and {len(services) - 10} more")
        except Exception as e:
            print(f"Error listing services: {e}")
            # Try alternate endpoint structure
            print("Trying to discover parcel-related services...")
            try:
                matches = await client.discover_layer_by_name("parcel")
                print(f"Found {len(matches)} parcel-related services:")
                for match in matches:
                    print(f"  - {match.get('name')}")
            except Exception as e2:
                print(f"Error discovering services: {e2}")

        # Step 2: Try to get layer schema
        print("\n[2] Attempting to get parcel layer schema...")

        # Try different possible paths for parcels
        possible_paths = [
            "Cadastral/Parcels/FeatureServer/0",
            "Parcels/FeatureServer/0",
            "Property/Parcels/FeatureServer/0",
            "Assessor/Parcels/FeatureServer/0",
        ]

        layer_path = None
        for path in possible_paths:
            try:
                print(f"  Trying: {path}...")
                fields = await client.get_layer_fields(path)
                if fields:
                    layer_path = path
                    print(f"  SUCCESS! Found {len(fields)} fields")
                    await client.print_layer_schema(path)
                    break
            except Exception as e:
                print(f"  Not found: {e}")
                continue

        # Step 3: Fetch sample parcels if we found a valid layer
        if layer_path:
            print("\n[3] Fetching sample parcels...")
            try:
                # Get the target boundary
                boundary = client.config.target_area.get_bounding_box()
                print(f"  Boundary: {boundary.to_esri_envelope()}")
                print(f"  Center: ({client.config.target_area.center_latitude}, "
                      f"{client.config.target_area.center_longitude})")

                # Fetch sample
                sample_gdf = await client.fetch_sample_parcels(n=10, boundary=boundary)

                if len(sample_gdf) > 0:
                    print(f"\n  Fetched {len(sample_gdf)} sample parcels")
                    print(f"  Columns: {list(sample_gdf.columns)}")
                    print("\n  Sample data (first 3 rows):")
                    # Show non-geometry columns
                    display_cols = [c for c in sample_gdf.columns if c != 'geometry'][:5]
                    if display_cols:
                        print(sample_gdf[display_cols].head(3).to_string())
                else:
                    print("  No parcels found in the target area")
                    print("  This might indicate the coordinates are outside Adams County")

            except Exception as e:
                print(f"  Error fetching parcels: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("\n[3] Skipping parcel fetch - no valid layer found")
            print("  You may need to explore the GIS services manually at:")
            print(f"  {client.config.base_url}")

    print("\n" + "="*60)
    print("Test Complete")
    print("="*60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
