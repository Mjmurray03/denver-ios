"""
File-based data loader for Adams County GIS data.

This module provides the AdamsCountyFileLoader class for loading
property data from local files (File Geodatabase, GeoPackage) rather
than querying the REST API. This approach is more reliable for
large datasets.

Data Sources:
- Parcels GDB: data/raw/adams_county/parcels_gdb/Parcels.gdb
  - Parcels (187K records, geometry)
  - PropertyValues (188K records, no geometry)
  - PropertyImprovements (167K records, no geometry)
  - PropertySales (752K records, no geometry)

- Building Footprints: data/raw/adams_county/building_footprints.gpkg
  - Building_Footprints (198K records, geometry)

All spatial data is in EPSG:2232 (Colorado State Plane North, feet)
"""

import logging
from pathlib import Path
from typing import Any, Optional

import fiona
import geopandas as gpd
import pandas as pd

from .exceptions import AcquisitionError, InvalidResponseError
from .models import BoundingBox, TargetArea

logger = logging.getLogger(__name__)

# Default data paths relative to project root
DEFAULT_PARCELS_GDB = Path("data/raw/adams_county/parcels_gdb/Parcels.gdb")
DEFAULT_BUILDINGS_GPKG = Path("data/raw/adams_county/building_footprints.gpkg")
DEFAULT_ZONING_GPKG = Path("data/raw/adams_county/zoning.gpkg")

# CRS definitions
SOURCE_CRS = "EPSG:2232"  # Colorado State Plane North (feet)
TARGET_CRS = "EPSG:4326"  # WGS84 (lat/lon)
METERS_CRS = "EPSG:26913"  # UTM Zone 13N (meters) for area calculations

# Layer names in the geodatabase
GDB_LAYERS = {
    "parcels": "Parcels",
    "values": "PropertyValues",
    "improvements": "PropertyImprovements",
    "sales": "PropertySales",
}

# Layer name in the building footprints GeoPackage
BUILDINGS_LAYER = "Building_Footprints"

# Layer name in the zoning GeoPackage
ZONING_LAYER = "Zoning"

# Field mapping for zoning data
ZONING_FIELD_MAP = {
    "ZONE_": "zoning_code",
    "CITY_NAME": "zoning_jurisdiction",
    "LINK": "zoning_regulations_url",
}

# Field mappings from source to standardized names
PARCEL_FIELD_MAP = {
    # Parcel identification
    "PIN": "pin",
    "PARCELNB": "parcel_number",
    # Address fields
    "streetno": "street_number",
    "streetdir": "street_direction",
    "streetname": "street_name",
    "streetsuf": "street_suffix",
    "streetpostdir": "street_post_direction",
    "streetalp": "street_alpha",
    "loccity": "city",
    "loczip": "zip_code",
    "concataddr1": "full_address",
    "concataddr2": "address_line2",
    # Owner information
    "ownername1": "owner_name1",
    "ownername2": "owner_name2",
    "ownernamefull": "owner_name_full",
    "owneraddress": "owner_address",
    "owneraddressfull": "owner_address_full",
    "ownercity": "owner_city",
    "ownerstate": "owner_state",
    "ownerzip": "owner_zip",
    # Parcel characteristics
    "subname": "subdivision_name",
}

VALUES_FIELD_MAP = {
    "accountno": "account_number",
    "areaid": "area_id",
    "parcelnb": "parcel_number",
    "accttype": "account_type",
    "actlandval": "actual_land_value",
    "actimpsval": "actual_improvement_value",
    "acttotalval": "actual_total_value",
    "asdlandval": "assessed_land_value",
    "asdimpsval": "assessed_improvement_value",
    "asdtotalval": "assessed_total_value",
    "lotsize": "lot_size",
    "lotmeasure": "lot_measure_unit",
    "firename": "fire_district",
    "schoolname": "school_district",
    "vacimp": "vacant_improved",
    "milllevy": "mill_levy",
    "pin": "pin",
}

IMPROVEMENTS_FIELD_MAP = {
    "accountno": "account_number",
    "areaid": "area_id",
    "parcelnb": "parcel_number",
    "bldgid": "building_id",
    "bltasdesc": "building_description",
    "proptype": "property_type",
    "yrblt": "year_built",
    "sf": "square_feet",
    "rooms": "rooms",
    "bedrooms": "bedrooms",
    "baths": "bathrooms",
    "attgarsf": "attached_garage_sf",
    "detgarsf": "detached_garage_sf",
    "bsmntsf": "basement_sf",
    "finbsmntsf": "finished_basement_sf",
    "pin": "pin",
    "exterior": "exterior_type",
}

BUILDINGS_FIELD_MAP = {
    "PIN": "pin",
    "PARCELNB": "parcel_number",
    "Bldg_ID": "building_id",
    "Unit": "unit",
    "Space_Nb": "space_number",
    "TRACT": "tract",
    "PropertyType": "property_type",
    "OccDesc": "occupancy_description",
    "created_date": "created_date",
    "last_edited_date": "last_edited_date",
}


class AdamsCountyFileLoader:
    """
    Loader for Adams County GIS data from local files.

    Loads parcel data from a File Geodatabase and building footprints
    from a GeoPackage. Provides filtering by geographic boundary and
    field name standardization.

    Usage:
        loader = AdamsCountyFileLoader()

        # List available layers
        layers = loader.list_gdb_layers()

        # Load all parcels
        parcels = loader.load_parcels()

        # Load parcels within a boundary
        boundary = BoundingBox.from_center_radius(39.6994321, -105.0099102, 1.32)
        parcels = loader.load_parcels_in_boundary(boundary)

        # Load building footprints
        buildings = loader.load_building_footprints()

    Attributes:
        parcels_gdb_path: Path to the parcels File Geodatabase.
        buildings_gpkg_path: Path to the building footprints GeoPackage.
        project_root: Project root directory for resolving relative paths.
    """

    def __init__(
        self,
        parcels_gdb_path: Optional[Path] = None,
        buildings_gpkg_path: Optional[Path] = None,
        zoning_gpkg_path: Optional[Path] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        """
        Initialize the file loader.

        Args:
            parcels_gdb_path: Path to parcels GDB. Defaults to project default.
            buildings_gpkg_path: Path to buildings GPKG. Defaults to project default.
            zoning_gpkg_path: Path to zoning GPKG. Defaults to project default.
            project_root: Project root for resolving relative paths.
                          Defaults to current working directory.
        """
        self.project_root = project_root or Path.cwd()

        self.parcels_gdb_path = parcels_gdb_path or (
            self.project_root / DEFAULT_PARCELS_GDB
        )
        self.buildings_gpkg_path = buildings_gpkg_path or (
            self.project_root / DEFAULT_BUILDINGS_GPKG
        )
        self.zoning_gpkg_path = zoning_gpkg_path or (
            self.project_root / DEFAULT_ZONING_GPKG
        )

        logger.info("Initialized file loader")
        logger.info("  Parcels GDB: %s", self.parcels_gdb_path)
        logger.info("  Buildings GPKG: %s", self.buildings_gpkg_path)
        logger.info("  Zoning GPKG: %s", self.zoning_gpkg_path)

    def _validate_path(self, path: Path, description: str) -> None:
        """
        Validate that a file path exists.

        Args:
            path: Path to validate.
            description: Human-readable description for error messages.

        Raises:
            AcquisitionError: If the path does not exist.
        """
        if not path.exists():
            raise AcquisitionError(
                f"{description} not found at: {path}. "
                "Please ensure the data files have been downloaded."
            )

    def list_gdb_layers(self) -> list[dict[str, Any]]:
        """
        List all layers in the parcels geodatabase.

        Returns:
            List of layer info dicts with name, geometry_type, record_count, and fields.

        Raises:
            AcquisitionError: If the geodatabase cannot be read.
        """
        self._validate_path(self.parcels_gdb_path, "Parcels geodatabase")

        try:
            layers = fiona.listlayers(str(self.parcels_gdb_path))
            result = []

            for layer_name in layers:
                with fiona.open(str(self.parcels_gdb_path), layer=layer_name) as src:
                    layer_info = {
                        "name": layer_name,
                        "geometry_type": src.schema.get("geometry"),
                        "record_count": len(src),
                        "crs": str(src.crs) if src.crs else None,
                        "fields": list(src.schema["properties"].keys()),
                    }
                    result.append(layer_info)

            logger.info("Found %d layers in geodatabase", len(result))
            return result

        except Exception as e:
            raise AcquisitionError(
                f"Failed to list geodatabase layers: {e}",
                cause=e,
            )

    def _standardize_columns(
        self, gdf: gpd.GeoDataFrame, field_map: dict[str, str]
    ) -> gpd.GeoDataFrame:
        """
        Rename columns using a field mapping.

        Args:
            gdf: GeoDataFrame to modify.
            field_map: Mapping from source names to standardized names.

        Returns:
            GeoDataFrame with renamed columns.
        """
        # Only rename columns that exist
        rename_map = {k: v for k, v in field_map.items() if k in gdf.columns}
        return gdf.rename(columns=rename_map)

    def _reproject_to_wgs84(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Reproject a GeoDataFrame to WGS84 (EPSG:4326).

        Args:
            gdf: GeoDataFrame to reproject.

        Returns:
            Reprojected GeoDataFrame.
        """
        if gdf.crs is None:
            logger.warning("GeoDataFrame has no CRS, assuming %s", SOURCE_CRS)
            gdf = gdf.set_crs(SOURCE_CRS)

        if gdf.crs.to_epsg() != 4326:
            logger.debug("Reprojecting from %s to %s", gdf.crs, TARGET_CRS)
            gdf = gdf.to_crs(TARGET_CRS)

        return gdf

    def _filter_by_boundary(
        self, gdf: gpd.GeoDataFrame, boundary: BoundingBox
    ) -> gpd.GeoDataFrame:
        """
        Filter a GeoDataFrame to features within a bounding box.

        Args:
            gdf: GeoDataFrame to filter.
            boundary: Bounding box to filter by.

        Returns:
            Filtered GeoDataFrame.
        """
        # Ensure we're in WGS84 for boundary comparison
        gdf = self._reproject_to_wgs84(gdf)

        # Create boundary geometry
        from shapely.geometry import box

        bbox_geom = box(boundary.min_x, boundary.min_y, boundary.max_x, boundary.max_y)

        # Filter using spatial intersection
        original_count = len(gdf)
        gdf = gdf[gdf.geometry.intersects(bbox_geom)]

        logger.info(
            "Filtered from %d to %d features (%.1f%%)",
            original_count,
            len(gdf),
            100 * len(gdf) / original_count if original_count > 0 else 0,
        )

        return gdf

    def load_parcels(
        self,
        standardize_fields: bool = True,
        reproject: bool = True,
    ) -> gpd.GeoDataFrame:
        """
        Load all parcels from the geodatabase.

        Args:
            standardize_fields: If True, rename fields to standard names.
            reproject: If True, reproject to WGS84 (EPSG:4326).

        Returns:
            GeoDataFrame with parcel polygons and attributes.

        Raises:
            AcquisitionError: If the data cannot be loaded.
        """
        self._validate_path(self.parcels_gdb_path, "Parcels geodatabase")

        logger.info("Loading parcels from %s", self.parcels_gdb_path)

        try:
            gdf = gpd.read_file(
                str(self.parcels_gdb_path),
                layer=GDB_LAYERS["parcels"],
            )
            logger.info("Loaded %d parcels with %d columns", len(gdf), len(gdf.columns))

            if standardize_fields:
                gdf = self._standardize_columns(gdf, PARCEL_FIELD_MAP)

            if reproject:
                gdf = self._reproject_to_wgs84(gdf)

            return gdf

        except Exception as e:
            raise AcquisitionError(
                f"Failed to load parcels: {e}",
                cause=e,
            )

    def load_parcels_in_boundary(
        self,
        boundary: BoundingBox,
        standardize_fields: bool = True,
    ) -> gpd.GeoDataFrame:
        """
        Load parcels within a geographic boundary.

        This is more efficient than loading all parcels and filtering,
        as it uses spatial filtering during the read operation when possible.

        Args:
            boundary: Bounding box to filter parcels.
            standardize_fields: If True, rename fields to standard names.

        Returns:
            GeoDataFrame with parcel polygons within the boundary.
        """
        self._validate_path(self.parcels_gdb_path, "Parcels geodatabase")

        logger.info(
            "Loading parcels in boundary: (%.4f, %.4f) to (%.4f, %.4f)",
            boundary.min_x,
            boundary.min_y,
            boundary.max_x,
            boundary.max_y,
        )

        try:
            # First load all parcels (GDB doesn't support bbox filter directly)
            gdf = gpd.read_file(
                str(self.parcels_gdb_path),
                layer=GDB_LAYERS["parcels"],
            )
            logger.info("Loaded %d total parcels", len(gdf))

            # Standardize fields before filtering
            if standardize_fields:
                gdf = self._standardize_columns(gdf, PARCEL_FIELD_MAP)

            # Filter by boundary (also reprojects to WGS84)
            gdf = self._filter_by_boundary(gdf, boundary)

            return gdf

        except Exception as e:
            raise AcquisitionError(
                f"Failed to load parcels in boundary: {e}",
                cause=e,
            )

    def load_property_values(
        self,
        standardize_fields: bool = True,
    ) -> pd.DataFrame:
        """
        Load property values data (no geometry).

        Args:
            standardize_fields: If True, rename fields to standard names.

        Returns:
            DataFrame with property valuation data.
        """
        self._validate_path(self.parcels_gdb_path, "Parcels geodatabase")

        logger.info("Loading property values from %s", self.parcels_gdb_path)

        try:
            # Use fiona to read non-spatial table
            with fiona.open(
                str(self.parcels_gdb_path), layer=GDB_LAYERS["values"]
            ) as src:
                records = [record["properties"] for record in src]

            df = pd.DataFrame(records)
            logger.info("Loaded %d property value records", len(df))

            if standardize_fields:
                rename_map = {k: v for k, v in VALUES_FIELD_MAP.items() if k in df.columns}
                df = df.rename(columns=rename_map)

            return df

        except Exception as e:
            raise AcquisitionError(
                f"Failed to load property values: {e}",
                cause=e,
            )

    def load_property_improvements(
        self,
        standardize_fields: bool = True,
    ) -> pd.DataFrame:
        """
        Load property improvements data (buildings on parcels).

        Args:
            standardize_fields: If True, rename fields to standard names.

        Returns:
            DataFrame with building/improvement data.
        """
        self._validate_path(self.parcels_gdb_path, "Parcels geodatabase")

        logger.info("Loading property improvements from %s", self.parcels_gdb_path)

        try:
            with fiona.open(
                str(self.parcels_gdb_path), layer=GDB_LAYERS["improvements"]
            ) as src:
                records = [record["properties"] for record in src]

            df = pd.DataFrame(records)
            logger.info("Loaded %d improvement records", len(df))

            if standardize_fields:
                rename_map = {
                    k: v for k, v in IMPROVEMENTS_FIELD_MAP.items() if k in df.columns
                }
                df = df.rename(columns=rename_map)

            return df

        except Exception as e:
            raise AcquisitionError(
                f"Failed to load property improvements: {e}",
                cause=e,
            )

    def load_building_footprints(
        self,
        standardize_fields: bool = True,
        reproject: bool = True,
    ) -> gpd.GeoDataFrame:
        """
        Load all building footprints from the GeoPackage.

        Args:
            standardize_fields: If True, rename fields to standard names.
            reproject: If True, reproject to WGS84 (EPSG:4326).

        Returns:
            GeoDataFrame with building footprint polygons.
        """
        self._validate_path(self.buildings_gpkg_path, "Building footprints GeoPackage")

        logger.info("Loading building footprints from %s", self.buildings_gpkg_path)

        try:
            gdf = gpd.read_file(
                str(self.buildings_gpkg_path),
                layer=BUILDINGS_LAYER,
            )
            logger.info(
                "Loaded %d building footprints with %d columns",
                len(gdf),
                len(gdf.columns),
            )

            if standardize_fields:
                gdf = self._standardize_columns(gdf, BUILDINGS_FIELD_MAP)

            if reproject:
                gdf = self._reproject_to_wgs84(gdf)

            return gdf

        except Exception as e:
            raise AcquisitionError(
                f"Failed to load building footprints: {e}",
                cause=e,
            )

    def load_buildings_in_boundary(
        self,
        boundary: BoundingBox,
        standardize_fields: bool = True,
    ) -> gpd.GeoDataFrame:
        """
        Load building footprints within a geographic boundary.

        Args:
            boundary: Bounding box to filter buildings.
            standardize_fields: If True, rename fields to standard names.

        Returns:
            GeoDataFrame with building footprints within the boundary.
        """
        self._validate_path(self.buildings_gpkg_path, "Building footprints GeoPackage")

        logger.info(
            "Loading buildings in boundary: (%.4f, %.4f) to (%.4f, %.4f)",
            boundary.min_x,
            boundary.min_y,
            boundary.max_x,
            boundary.max_y,
        )

        try:
            # GeoPackage supports bbox filtering via Fiona
            # But we need to transform the boundary to the source CRS first
            from pyproj import Transformer
            from shapely.geometry import box

            # Transform boundary from WGS84 to source CRS
            transformer = Transformer.from_crs(TARGET_CRS, SOURCE_CRS, always_xy=True)
            min_x, min_y = transformer.transform(boundary.min_x, boundary.min_y)
            max_x, max_y = transformer.transform(boundary.max_x, boundary.max_y)

            # Load with bbox filter
            gdf = gpd.read_file(
                str(self.buildings_gpkg_path),
                layer=BUILDINGS_LAYER,
                bbox=(min_x, min_y, max_x, max_y),
            )
            logger.info("Loaded %d buildings in boundary", len(gdf))

            if standardize_fields:
                gdf = self._standardize_columns(gdf, BUILDINGS_FIELD_MAP)

            # Reproject to WGS84
            gdf = self._reproject_to_wgs84(gdf)

            return gdf

        except Exception as e:
            raise AcquisitionError(
                f"Failed to load buildings in boundary: {e}",
                cause=e,
            )

    def load_parcels_with_values(
        self,
        boundary: Optional[BoundingBox] = None,
        standardize_fields: bool = True,
    ) -> gpd.GeoDataFrame:
        """
        Load parcels joined with property values.

        This combines the spatial parcel data with valuation information.

        Args:
            boundary: Optional bounding box to filter parcels.
            standardize_fields: If True, rename fields to standard names.

        Returns:
            GeoDataFrame with parcels and their property values.
        """
        # Load parcels
        if boundary:
            parcels = self.load_parcels_in_boundary(boundary, standardize_fields)
        else:
            parcels = self.load_parcels(standardize_fields, reproject=True)

        # Load values
        values = self.load_property_values(standardize_fields)

        # Determine join column
        join_col = "parcel_number" if standardize_fields else "PARCELNB"
        values_join_col = "parcel_number" if standardize_fields else "parcelnb"

        # Join on parcel number
        logger.info("Joining %d parcels with %d value records", len(parcels), len(values))

        # Rename values join column to match parcels if needed
        if join_col != values_join_col:
            values = values.rename(columns={values_join_col: join_col})

        # Perform the join
        result = parcels.merge(
            values,
            on=join_col,
            how="left",
            suffixes=("", "_values"),
        )

        logger.info("Joined result has %d records", len(result))
        return result

    def load_zoning(
        self,
        standardize_fields: bool = True,
        reproject: bool = True,
    ) -> gpd.GeoDataFrame:
        """
        Load all zoning polygons from the GeoPackage.

        Args:
            standardize_fields: If True, rename fields to standard names.
            reproject: If True, reproject to WGS84 (EPSG:4326).

        Returns:
            GeoDataFrame with zoning polygons and attributes.
        """
        self._validate_path(self.zoning_gpkg_path, "Zoning GeoPackage")

        logger.info("Loading zoning from %s", self.zoning_gpkg_path)

        try:
            gdf = gpd.read_file(
                str(self.zoning_gpkg_path),
                layer=ZONING_LAYER,
            )
            logger.info(
                "Loaded %d zoning polygons with %d columns",
                len(gdf),
                len(gdf.columns),
            )

            if standardize_fields:
                gdf = self._standardize_columns(gdf, ZONING_FIELD_MAP)

            if reproject:
                gdf = self._reproject_to_wgs84(gdf)

            return gdf

        except Exception as e:
            raise AcquisitionError(
                f"Failed to load zoning: {e}",
                cause=e,
            )

    def load_zoning_in_boundary(
        self,
        boundary: BoundingBox,
        standardize_fields: bool = True,
    ) -> gpd.GeoDataFrame:
        """
        Load zoning polygons within a geographic boundary.

        Args:
            boundary: Bounding box to filter zoning areas.
            standardize_fields: If True, rename fields to standard names.

        Returns:
            GeoDataFrame with zoning polygons within the boundary.
        """
        self._validate_path(self.zoning_gpkg_path, "Zoning GeoPackage")

        logger.info(
            "Loading zoning in boundary: (%.4f, %.4f) to (%.4f, %.4f)",
            boundary.min_x,
            boundary.min_y,
            boundary.max_x,
            boundary.max_y,
        )

        try:
            # Transform boundary to source CRS for bbox filter
            from pyproj import Transformer

            transformer = Transformer.from_crs(TARGET_CRS, SOURCE_CRS, always_xy=True)
            min_x, min_y = transformer.transform(boundary.min_x, boundary.min_y)
            max_x, max_y = transformer.transform(boundary.max_x, boundary.max_y)

            # Load with bbox filter
            gdf = gpd.read_file(
                str(self.zoning_gpkg_path),
                layer=ZONING_LAYER,
                bbox=(min_x, min_y, max_x, max_y),
            )
            logger.info("Loaded %d zoning polygons in boundary", len(gdf))

            if standardize_fields:
                gdf = self._standardize_columns(gdf, ZONING_FIELD_MAP)

            # Reproject to WGS84
            gdf = self._reproject_to_wgs84(gdf)

            return gdf

        except Exception as e:
            raise AcquisitionError(
                f"Failed to load zoning in boundary: {e}",
                cause=e,
            )


def main() -> None:
    """
    Test function for Adams County file loader.

    Demonstrates loading data from local files and exploring
    the geodatabase structure.
    """
    import sys

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("\n" + "=" * 60)
    print("Adams County File Loader Test")
    print("=" * 60)

    try:
        loader = AdamsCountyFileLoader()

        # Step 1: List GDB layers
        print("\n[1] Geodatabase Layers")
        print("-" * 40)
        layers = loader.list_gdb_layers()
        for layer in layers:
            geom = layer["geometry_type"] or "None (table)"
            print(f"  {layer['name']}: {layer['record_count']:,} records, {geom}")
            print(f"      Fields: {', '.join(layer['fields'][:5])}...")

        # Step 2: Load sample parcels
        print("\n[2] Sample Parcels (first 5)")
        print("-" * 40)
        parcels = loader.load_parcels(standardize_fields=True, reproject=True)
        print(f"  Total parcels: {len(parcels):,}")
        print(f"  Columns: {list(parcels.columns)}")
        print(f"  CRS: {parcels.crs}")

        # Show sample data
        sample_cols = ["pin", "parcel_number", "full_address", "city"]
        available_cols = [c for c in sample_cols if c in parcels.columns]
        if available_cols:
            print("\n  Sample data:")
            print(parcels[available_cols].head().to_string())

        # Step 3: Load parcels in target boundary
        print("\n[3] Parcels in Target Area")
        print("-" * 40)
        target = TargetArea()
        boundary = target.get_bounding_box()
        print(f"  Center: ({target.center_latitude}, {target.center_longitude})")
        print(f"  Radius: {target.radius_km} km")
        print(f"  Boundary: {boundary.to_esri_envelope()}")

        target_parcels = loader.load_parcels_in_boundary(boundary)
        print(f"  Parcels in area: {len(target_parcels):,}")

        if len(target_parcels) > 0:
            print("\n  Sample target parcels:")
            print(target_parcels[available_cols].head(3).to_string())

        # Step 4: Load building footprints
        print("\n[4] Building Footprints")
        print("-" * 40)
        buildings = loader.load_building_footprints()
        print(f"  Total buildings: {len(buildings):,}")
        print(f"  Columns: {list(buildings.columns)}")

        # Load buildings in target area
        target_buildings = loader.load_buildings_in_boundary(boundary)
        print(f"  Buildings in target area: {len(target_buildings):,}")

        # Step 5: Load property values
        print("\n[5] Property Values")
        print("-" * 40)
        values = loader.load_property_values()
        print(f"  Total value records: {len(values):,}")

        value_cols = ["parcel_number", "actual_total_value", "lot_size"]
        avail_value_cols = [c for c in value_cols if c in values.columns]
        if avail_value_cols:
            print("\n  Sample values (non-null):")
            sample = values[values["actual_total_value"].notna()][avail_value_cols].head()
            print(sample.to_string())

        # Step 6: Load joined data
        print("\n[6] Parcels with Values (Target Area)")
        print("-" * 40)
        joined = loader.load_parcels_with_values(boundary)
        print(f"  Joined records: {len(joined):,}")

        # Show value statistics for target area
        if "actual_total_value" in joined.columns:
            values_present = joined["actual_total_value"].notna().sum()
            print(f"  With values: {values_present:,}")
            if values_present > 0:
                print(f"  Value range: ${joined['actual_total_value'].min():,.0f} - "
                      f"${joined['actual_total_value'].max():,.0f}")
                print(f"  Mean value: ${joined['actual_total_value'].mean():,.0f}")

    except AcquisitionError as e:
        print(f"\nError: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Test Complete")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
