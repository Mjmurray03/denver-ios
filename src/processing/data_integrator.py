"""
Data integration module for Adams County property datasets.

This module provides the PropertyDataIntegrator class that combines
multiple property datasets (parcels, values, improvements, sales,
building footprints) into a unified GeoDataFrame for analysis.

Join Key Analysis:
- Parcels: PIN (no leading zero), PARCELNB (with leading zero '0')
- PropertyValues: pin, parcelnb (matches Parcels format)
- PropertyImprovements: pin, parcelnb
- PropertySales: pin, parcelnb
- Building_Footprints: PIN, PARCELNB (matches Parcels format)

Strategy: Use parcelnb/PARCELNB as the primary join key (more consistent).
"""

import logging
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
import numpy as np

from src.acquisition import AdamsCountyFileLoader, BoundingBox, TargetArea

logger = logging.getLogger(__name__)

# Column name mappings for standardization
PARCEL_COLUMNS = {
    "PIN": "pin",
    "PARCELNB": "parcel_id",
}

VALUES_COLUMNS = {
    "parcelnb": "parcel_id",
    "pin": "pin",
    "actlandval": "actual_land_value",
    "actimpsval": "actual_improvement_value",
    "acttotalval": "actual_total_value",
    "asdlandval": "assessed_land_value",
    "asdimpsval": "assessed_improvement_value",
    "asdtotalval": "assessed_total_value",
    "lotsize": "lot_size",
    "lotmeasure": "lot_measure_unit",
    "vacimp": "vacant_improved",
    "milllevy": "mill_levy",
}

IMPROVEMENTS_COLUMNS = {
    "parcelnb": "parcel_id",
    "pin": "pin",
    "bldgid": "building_id",
    "bltasdesc": "building_description",
    "proptype": "property_type",
    "yrblt": "year_built",
    "sf": "building_sqft",
    "rooms": "rooms",
    "bedrooms": "bedrooms",
    "baths": "bathrooms",
    "attgarsf": "attached_garage_sqft",
    "detgarsf": "detached_garage_sqft",
    "bsmntsf": "basement_sqft",
    "finbsmntsf": "finished_basement_sqft",
    "exterior": "exterior_type",
}

SALES_COLUMNS = {
    "parcelnb": "parcel_id",
    "pin": "pin",
    "recptno": "receipt_number",
    "deedtype": "deed_type",
    "salesp": "sale_price",
    "saledt": "sale_date",
    "grantor": "seller",
    "grantee": "buyer",
}

BUILDINGS_COLUMNS = {
    "PIN": "pin",
    "PARCELNB": "parcel_id",
    "Bldg_ID": "footprint_building_id",
    "PropertyType": "footprint_property_type",
    "OccDesc": "occupancy_description",
}


class PropertyDataIntegrator:
    """
    Integrates multiple Adams County property datasets into a unified dataset.

    Combines parcels (with geometry), property values, improvements, sales
    history, and building footprints into a single GeoDataFrame suitable
    for IOS property analysis.

    Usage:
        integrator = PropertyDataIntegrator()

        # Explore join keys
        integrator.explore_join_keys()

        # Create unified dataset for target area
        target = TargetArea()
        unified = integrator.create_unified_dataset(target.get_bounding_box())

    Attributes:
        loader: AdamsCountyFileLoader instance for loading raw data.
    """

    def __init__(
        self,
        loader: Optional[AdamsCountyFileLoader] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        """
        Initialize the data integrator.

        Args:
            loader: Optional AdamsCountyFileLoader. Created if not provided.
            project_root: Project root directory for file paths.
        """
        self.loader = loader or AdamsCountyFileLoader(project_root=project_root)
        logger.info("Initialized PropertyDataIntegrator")

    def explore_join_keys(self) -> dict[str, dict[str, Any]]:
        """
        Explore and report potential join keys in each dataset.

        Analyzes each dataset to identify join key columns and their
        value formats to help determine the best join strategy.

        Returns:
            Dictionary with dataset names as keys and join key info as values.
        """
        import fiona

        results = {}

        datasets = [
            ("Parcels", self.loader.parcels_gdb_path, "Parcels"),
            ("PropertyValues", self.loader.parcels_gdb_path, "PropertyValues"),
            ("PropertyImprovements", self.loader.parcels_gdb_path, "PropertyImprovements"),
            ("PropertySales", self.loader.parcels_gdb_path, "PropertySales"),
            ("Building_Footprints", self.loader.buildings_gpkg_path, "Building_Footprints"),
        ]

        print("\n" + "=" * 70)
        print("JOIN KEY ANALYSIS")
        print("=" * 70)

        for name, path, layer in datasets:
            print(f"\n{name}:")
            print("-" * 50)

            with fiona.open(str(path), layer=layer) as src:
                fields = list(src.schema["properties"].keys())

                # Find potential join keys
                key_patterns = ["PIN", "PARCEL", "ACCOUNT", "ID"]
                potential_keys = [
                    f for f in fields
                    if any(k in f.upper() for k in key_patterns)
                ]

                # Get sample values
                samples = {}
                rec = next(iter(src))
                for key in potential_keys:
                    samples[key] = rec["properties"].get(key)

                results[name] = {
                    "potential_keys": potential_keys,
                    "sample_values": samples,
                    "record_count": len(src),
                }

                print(f"  Record count: {len(src):,}")
                print(f"  Potential join columns: {potential_keys}")
                print("  Sample values:")
                for key, val in samples.items():
                    print(f"    {key}: {val!r}")

        print("\n" + "=" * 70)
        print("RECOMMENDED JOIN STRATEGY:")
        print("  Primary key: parcel_id (from PARCELNB/parcelnb)")
        print("  All datasets have parcelnb with consistent format (leading '0')")
        print("=" * 70 + "\n")

        return results

    def _standardize_parcel_id(self, df: pd.DataFrame, source_col: str) -> pd.DataFrame:
        """
        Standardize parcel ID column to ensure consistent join key format.

        Args:
            df: DataFrame to modify.
            source_col: Source column name containing parcel ID.

        Returns:
            DataFrame with standardized parcel_id column.
        """
        if source_col in df.columns:
            # Ensure string type and strip whitespace
            df["parcel_id"] = df[source_col].astype(str).str.strip()

            # Ensure leading zero for consistency (13-digit format)
            df["parcel_id"] = df["parcel_id"].apply(
                lambda x: x.zfill(13) if x and len(x) < 13 else x
            )

        return df

    def load_and_merge_property_data(
        self,
        boundary: Optional[BoundingBox] = None,
    ) -> gpd.GeoDataFrame:
        """
        Load and merge parcels with property values and improvements.

        This performs a tabular merge (not spatial) using parcel_id as the
        join key. Does not include building footprints (use spatial_join_buildings).

        Args:
            boundary: Optional bounding box to filter parcels spatially.

        Returns:
            GeoDataFrame with parcels merged with values and improvements.
        """
        logger.info("Loading and merging property data...")

        # 1. Load parcels (with geometry)
        if boundary:
            parcels = self.loader.load_parcels_in_boundary(
                boundary, standardize_fields=False
            )
        else:
            parcels = self.loader.load_parcels(
                standardize_fields=False, reproject=True
            )

        logger.info("Loaded %d parcels", len(parcels))

        # Standardize parcel_id
        parcels = self._standardize_parcel_id(parcels, "PARCELNB")

        # 2. Load property values
        values = self.loader.load_property_values(standardize_fields=False)
        logger.info("Loaded %d property value records", len(values))

        # Rename columns (exclude parcelnb - we'll standardize it separately)
        val_rename = {
            k: v for k, v in VALUES_COLUMNS.items()
            if k in values.columns and k != "parcelnb"
        }
        values = values.rename(columns=val_rename)

        # Standardize parcel_id from parcelnb
        values = self._standardize_parcel_id(values, "parcelnb")

        # Select key value columns
        value_cols = [
            "parcel_id",
            "actual_land_value",
            "actual_improvement_value",
            "actual_total_value",
            "assessed_total_value",
            "lot_size",
            "lot_measure_unit",
            "vacant_improved",
        ]
        value_cols = [c for c in value_cols if c in values.columns]
        values = values[value_cols].drop_duplicates(subset=["parcel_id"])

        # 3. Load property improvements
        improvements = self.loader.load_property_improvements(standardize_fields=False)
        logger.info("Loaded %d improvement records", len(improvements))

        # Rename columns first (before adding parcel_id)
        imp_rename = {
            k: v for k, v in IMPROVEMENTS_COLUMNS.items()
            if k in improvements.columns and k != "parcelnb"  # Don't rename parcelnb yet
        }
        improvements = improvements.rename(columns=imp_rename)

        # Now standardize parcel_id from parcelnb
        improvements = self._standardize_parcel_id(improvements, "parcelnb")

        # Aggregate improvements per parcel (there can be multiple buildings)
        agg_cols = {}
        if "building_sqft" in improvements.columns:
            agg_cols["building_sqft"] = "sum"
        if "year_built" in improvements.columns:
            agg_cols["year_built"] = "min"  # Oldest building
        if "property_type" in improvements.columns:
            agg_cols["property_type"] = "first"
        if "building_description" in improvements.columns:
            agg_cols["building_description"] = "first"
        if "rooms" in improvements.columns:
            agg_cols["rooms"] = "sum"
        if "bedrooms" in improvements.columns:
            agg_cols["bedrooms"] = "sum"
        if "bathrooms" in improvements.columns:
            agg_cols["bathrooms"] = "sum"

        imp_agg = improvements.groupby("parcel_id").agg(agg_cols).reset_index()

        imp_agg = imp_agg.rename(columns={
            "building_sqft": "total_building_sqft",
            "year_built": "oldest_year_built",
        })

        # Count buildings per parcel
        imp_counts = improvements.groupby("parcel_id").size().reset_index(name="improvement_count")
        imp_agg = imp_agg.merge(imp_counts, on="parcel_id", how="left")

        # 4. Merge datasets
        logger.info("Merging parcels with values...")
        merged = parcels.merge(values, on="parcel_id", how="left")

        logger.info("Merging with improvements...")
        merged = merged.merge(imp_agg, on="parcel_id", how="left")

        logger.info(
            "Merged dataset: %d records, %d columns",
            len(merged),
            len(merged.columns),
        )

        return merged

    def load_latest_sales(
        self,
        parcel_ids: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        Load the most recent sale for each parcel.

        Args:
            parcel_ids: Optional list of parcel IDs to filter.

        Returns:
            DataFrame with one row per parcel showing latest sale info.
        """
        logger.info("Loading sales data...")

        # Load all sales
        import fiona

        records = []
        with fiona.open(
            str(self.loader.parcels_gdb_path), layer="PropertySales"
        ) as src:
            for rec in src:
                records.append(rec["properties"])

        sales = pd.DataFrame(records)
        sales = self._standardize_parcel_id(sales, "parcelnb")

        # Rename columns (exclude parcelnb - already standardized to parcel_id)
        sales_rename = {
            k: v for k, v in SALES_COLUMNS.items()
            if k in sales.columns and k != "parcelnb"
        }
        sales = sales.rename(columns=sales_rename)

        logger.info("Loaded %d sales records", len(sales))

        # Filter to specific parcels if provided
        if parcel_ids:
            sales = sales[sales["parcel_id"].isin(parcel_ids)]
            logger.info("Filtered to %d sales for specified parcels", len(sales))

        # Convert sale_date to datetime
        if "sale_date" in sales.columns:
            sales["sale_date"] = pd.to_datetime(sales["sale_date"], errors="coerce")

        # Get most recent sale per parcel
        if "sale_date" in sales.columns:
            sales = sales.sort_values("sale_date", ascending=False)
            latest_sales = sales.groupby("parcel_id").first().reset_index()
        else:
            latest_sales = sales.drop_duplicates(subset=["parcel_id"])

        # Select relevant columns
        sale_cols = ["parcel_id", "sale_price", "sale_date", "deed_type", "buyer", "seller"]
        sale_cols = [c for c in sale_cols if c in latest_sales.columns]
        latest_sales = latest_sales[sale_cols]

        # Rename for clarity
        latest_sales = latest_sales.rename(columns={
            "sale_price": "last_sale_price",
            "sale_date": "last_sale_date",
            "deed_type": "last_deed_type",
        })

        logger.info("Found latest sales for %d parcels", len(latest_sales))
        return latest_sales

    def spatial_join_buildings(
        self,
        parcels_gdf: gpd.GeoDataFrame,
        buildings_gdf: Optional[gpd.GeoDataFrame] = None,
        boundary: Optional[BoundingBox] = None,
    ) -> gpd.GeoDataFrame:
        """
        Spatially join building footprints to parcels and calculate coverage.

        Args:
            parcels_gdf: GeoDataFrame of parcels (must have geometry).
            buildings_gdf: Optional GeoDataFrame of buildings. Loaded if not provided.
            boundary: Optional boundary to filter buildings.

        Returns:
            GeoDataFrame with building summary columns added to parcels.
        """
        logger.info("Performing spatial join of buildings to parcels...")

        # Load buildings if not provided
        if buildings_gdf is None:
            if boundary:
                buildings_gdf = self.loader.load_buildings_in_boundary(
                    boundary, standardize_fields=False
                )
            else:
                buildings_gdf = self.loader.load_building_footprints(
                    standardize_fields=False, reproject=True
                )

        logger.info("Using %d building footprints", len(buildings_gdf))

        if len(buildings_gdf) == 0:
            logger.warning("No buildings to join")
            parcels_gdf["building_footprint_count"] = 0
            parcels_gdf["building_footprint_sqft"] = 0.0
            parcels_gdf["building_coverage_pct"] = 0.0
            return parcels_gdf

        # Ensure same CRS
        if parcels_gdf.crs != buildings_gdf.crs:
            buildings_gdf = buildings_gdf.to_crs(parcels_gdf.crs)

        # Calculate building footprint areas (in square feet)
        # Project to meters CRS for accurate area calculation
        buildings_projected = buildings_gdf.to_crs("EPSG:26913")
        buildings_gdf["footprint_area_sqm"] = buildings_projected.geometry.area
        buildings_gdf["footprint_area_sqft"] = buildings_gdf["footprint_area_sqm"] * 10.764

        # Spatial join - find which parcel each building belongs to
        buildings_with_parcel = gpd.sjoin(
            buildings_gdf,
            parcels_gdf[["parcel_id", "geometry"]],
            how="left",
            predicate="within",
        )

        # Aggregate building stats per parcel
        building_stats = buildings_with_parcel.groupby("parcel_id").agg({
            "footprint_area_sqft": ["count", "sum"],
        }).reset_index()

        building_stats.columns = [
            "parcel_id",
            "building_footprint_count",
            "building_footprint_sqft",
        ]

        # Merge back to parcels
        result = parcels_gdf.merge(building_stats, on="parcel_id", how="left")

        # Fill NaN with 0 for parcels without buildings
        result["building_footprint_count"] = result["building_footprint_count"].fillna(0).astype(int)
        result["building_footprint_sqft"] = result["building_footprint_sqft"].fillna(0.0)

        # Calculate parcel area and coverage percentage
        parcels_projected = result.to_crs("EPSG:26913")
        result["parcel_area_sqft"] = parcels_projected.geometry.area * 10.764

        result["building_coverage_pct"] = np.where(
            result["parcel_area_sqft"] > 0,
            (result["building_footprint_sqft"] / result["parcel_area_sqft"]) * 100,
            0.0,
        )

        logger.info(
            "Building stats: %d parcels with buildings, avg coverage %.1f%%",
            (result["building_footprint_count"] > 0).sum(),
            result["building_coverage_pct"].mean(),
        )

        return result

    def spatial_join_zoning(
        self,
        parcels_gdf: gpd.GeoDataFrame,
        zoning_gdf: Optional[gpd.GeoDataFrame] = None,
        boundary: Optional[BoundingBox] = None,
    ) -> gpd.GeoDataFrame:
        """
        Spatially join zoning data to parcels based on parcel centroid.

        For parcels that span multiple zones, uses the parcel centroid to
        determine the primary zone assignment.

        Args:
            parcels_gdf: GeoDataFrame of parcels (must have geometry).
            zoning_gdf: Optional GeoDataFrame of zoning polygons. Loaded if not provided.
            boundary: Optional boundary to filter zoning polygons.

        Returns:
            GeoDataFrame with zoning columns added to parcels.
        """
        logger.info("Performing spatial join of zoning to parcels...")

        # Load zoning if not provided
        if zoning_gdf is None:
            if boundary:
                zoning_gdf = self.loader.load_zoning_in_boundary(
                    boundary, standardize_fields=True
                )
            else:
                zoning_gdf = self.loader.load_zoning(
                    standardize_fields=True, reproject=True
                )

        logger.info("Using %d zoning polygons", len(zoning_gdf))

        if len(zoning_gdf) == 0:
            logger.warning("No zoning data to join")
            parcels_gdf["zoning_code"] = None
            parcels_gdf["zoning_jurisdiction"] = None
            return parcels_gdf

        # Ensure same CRS
        if parcels_gdf.crs != zoning_gdf.crs:
            zoning_gdf = zoning_gdf.to_crs(parcels_gdf.crs)

        # Create parcel centroids for point-in-polygon join
        parcels_centroids = parcels_gdf.copy()
        parcels_centroids["centroid_geom"] = parcels_centroids.geometry.centroid
        parcels_centroids = parcels_centroids.set_geometry("centroid_geom")

        # Spatial join - find which zone each parcel centroid falls into
        zoning_cols = ["zoning_code", "zoning_jurisdiction", "geometry"]
        zoning_cols = [c for c in zoning_cols if c in zoning_gdf.columns]

        parcels_with_zoning = gpd.sjoin(
            parcels_centroids,
            zoning_gdf[zoning_cols],
            how="left",
            predicate="within",
        )

        # Handle duplicates (parcel centroid on zone boundary - take first)
        parcels_with_zoning = parcels_with_zoning.drop_duplicates(subset=["parcel_id"])

        # Restore original geometry
        parcels_with_zoning = parcels_with_zoning.set_geometry("geometry")
        parcels_with_zoning = parcels_with_zoning.drop(columns=["centroid_geom", "index_right"], errors="ignore")

        # Count parcels with zoning
        has_zoning = parcels_with_zoning["zoning_code"].notna().sum()
        logger.info(
            "Zoning assigned: %d parcels with zoning (%.1f%%)",
            has_zoning,
            100 * has_zoning / len(parcels_with_zoning) if len(parcels_with_zoning) > 0 else 0,
        )

        # Log zoning distribution
        if has_zoning > 0:
            zoning_dist = parcels_with_zoning["zoning_code"].value_counts().head(10)
            logger.info("Top zoning codes:")
            for zone, count in zoning_dist.items():
                logger.info("  %s: %d parcels", zone, count)

        return parcels_with_zoning

    def create_unified_dataset(
        self,
        boundary: Optional[BoundingBox] = None,
        include_sales: bool = True,
        include_buildings: bool = True,
        include_zoning: bool = True,
    ) -> gpd.GeoDataFrame:
        """
        Create a fully unified property dataset with all available information.

        This is the main entry point that combines:
        - Parcel geometry and basic info
        - Property values (assessed values, lot size)
        - Property improvements (buildings, year built, sqft)
        - Latest sales (optional)
        - Building footprint summary (optional, requires spatial join)
        - Zoning data (optional, requires spatial join)

        Args:
            boundary: Optional bounding box to filter the area of interest.
            include_sales: Whether to include latest sale information.
            include_buildings: Whether to include building footprint analysis.
            include_zoning: Whether to include zoning data via spatial join.

        Returns:
            Unified GeoDataFrame with all property information.
        """
        logger.info("Creating unified dataset...")
        logger.info(
            "  Options: include_sales=%s, include_buildings=%s, include_zoning=%s",
            include_sales,
            include_buildings,
            include_zoning,
        )

        # Step 1: Load and merge parcels, values, improvements
        unified = self.load_and_merge_property_data(boundary)

        # Step 2: Add latest sales if requested
        if include_sales:
            parcel_ids = unified["parcel_id"].unique().tolist()
            sales = self.load_latest_sales(parcel_ids)
            unified = unified.merge(sales, on="parcel_id", how="left")
            logger.info("Added sales data for %d parcels", sales["parcel_id"].nunique())

        # Step 3: Spatial join buildings if requested
        if include_buildings:
            unified = self.spatial_join_buildings(unified, boundary=boundary)

        # Step 4: Spatial join zoning if requested
        if include_zoning:
            unified = self.spatial_join_zoning(unified, boundary=boundary)

        # Step 5: Clean up and reorder columns
        # Put key columns first
        key_cols = [
            "parcel_id",
            "pin",
            "geometry",
        ]

        # Address columns
        address_cols = [
            "concataddr1",
            "loccity",
            "loczip",
        ]

        # Zoning columns
        zoning_cols = [
            "zoning_code",
            "zoning_jurisdiction",
        ]

        # Value columns
        value_cols = [
            "actual_total_value",
            "actual_land_value",
            "actual_improvement_value",
            "assessed_total_value",
            "lot_size",
        ]

        # Building columns
        building_cols = [
            "total_building_sqft",
            "oldest_year_built",
            "property_type",
            "improvement_count",
            "building_footprint_count",
            "building_footprint_sqft",
            "building_coverage_pct",
            "parcel_area_sqft",
        ]

        # Sales columns
        sale_cols = [
            "last_sale_price",
            "last_sale_date",
        ]

        # Build ordered column list
        ordered_cols = []
        for col_list in [key_cols, address_cols, zoning_cols, value_cols, building_cols, sale_cols]:
            for col in col_list:
                if col in unified.columns and col not in ordered_cols:
                    ordered_cols.append(col)

        # Add remaining columns
        remaining = [c for c in unified.columns if c not in ordered_cols]
        ordered_cols.extend(remaining)

        unified = unified[ordered_cols]

        logger.info(
            "Unified dataset complete: %d records, %d columns",
            len(unified),
            len(unified.columns),
        )

        return unified


def main() -> None:
    """
    Test the data integrator with sample data.
    """
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("\n" + "=" * 70)
    print("PROPERTY DATA INTEGRATOR TEST")
    print("=" * 70)

    try:
        integrator = PropertyDataIntegrator()

        # Step 1: Explore join keys
        print("\n[1] Exploring Join Keys")
        print("-" * 50)
        integrator.explore_join_keys()

        # Step 2: Create unified dataset for target area
        print("\n[2] Creating Unified Dataset for Target Area")
        print("-" * 50)

        target = TargetArea()
        boundary = target.get_bounding_box()
        print(f"Center: ({target.center_latitude}, {target.center_longitude})")
        print(f"Radius: {target.radius_km} km")
        print(f"Boundary: {boundary.to_esri_envelope()}")

        unified = integrator.create_unified_dataset(
            boundary=boundary,
            include_sales=True,
            include_buildings=True,
        )

        print(f"\nUnified dataset shape: {unified.shape}")
        print(f"Columns: {list(unified.columns)}")

        # Step 3: Show sample data
        print("\n[3] Sample Data")
        print("-" * 50)

        display_cols = [
            "parcel_id",
            "actual_total_value",
            "total_building_sqft",
            "building_coverage_pct",
            "last_sale_price",
        ]
        display_cols = [c for c in display_cols if c in unified.columns]

        if len(unified) > 0:
            print(unified[display_cols].head(10).to_string())

            # Summary statistics
            print("\n[4] Summary Statistics")
            print("-" * 50)

            if "actual_total_value" in unified.columns:
                values = unified["actual_total_value"].dropna()
                print(f"Property Values:")
                print(f"  Count: {len(values):,}")
                print(f"  Mean: ${values.mean():,.0f}")
                print(f"  Median: ${values.median():,.0f}")
                print(f"  Range: ${values.min():,.0f} - ${values.max():,.0f}")

            if "building_coverage_pct" in unified.columns:
                coverage = unified["building_coverage_pct"]
                print(f"\nBuilding Coverage:")
                print(f"  Mean: {coverage.mean():.1f}%")
                print(f"  Parcels with buildings: {(coverage > 0).sum():,}")
                print(f"  Parcels without buildings: {(coverage == 0).sum():,}")

                # IOS scoring hint
                low_coverage = unified[coverage <= 25]
                print(f"\nIOS Candidates (<=25% coverage): {len(low_coverage):,}")

        else:
            print("No parcels found in target area")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
