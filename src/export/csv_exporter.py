"""
CSV Exporter for CRM Import.

Generates clean CSV files with standardized column names
suitable for importing into CRM systems.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class CSVExporter:
    """Export property data to CSV format for CRM import."""

    def __init__(self, output_dir: Path | str = "deliverables"):
        """
        Initialize the CSV exporter.

        Args:
            output_dir: Directory for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export(
        self,
        scored_df: pd.DataFrame,
        filename: str = "denver_ios_crm.csv",
        include_all: bool = True,
        min_grade: Optional[str] = None,
    ) -> Path:
        """
        Export scored properties to a CSV file for CRM import.

        Args:
            scored_df: DataFrame with IOS scores and property data
            filename: Output filename
            include_all: If True, include all properties; if False, use min_grade filter
            min_grade: Minimum grade to include (A, B, C, D, or F)

        Returns:
            Path to the generated CSV file
        """
        output_path = self.output_dir / filename
        logger.info(f"Generating CRM CSV: {output_path}")

        df = scored_df.copy()

        # Filter by grade if specified
        if not include_all and min_grade:
            grade_col = self._find_column(df, "ios_grade", "grade")
            if grade_col:
                grade_order = ["A", "B", "C", "D", "F"]
                min_idx = grade_order.index(min_grade.upper()) if min_grade.upper() in grade_order else 4
                valid_grades = grade_order[: min_idx + 1]
                df = df[df[grade_col].isin(valid_grades)]
                logger.info(f"Filtered to grades {valid_grades}: {len(df)} properties")

        # Prepare comprehensive CRM-friendly DataFrame with ALL columns
        crm_df = self._prepare_crm_dataframe(df)

        # Sort by score descending
        if "ios_score" in crm_df.columns:
            crm_df = crm_df.sort_values("ios_score", ascending=False)

        # Save to CSV
        crm_df.to_csv(output_path, index=False)
        logger.info(f"CRM CSV saved: {output_path} ({len(crm_df)} records, {len(crm_df.columns)} columns)")

        return output_path

    def _prepare_crm_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare a DataFrame with clean CRM-friendly column names including ALL data."""

        # Column renaming map - maps source columns to clean CRM names
        # Priority columns come first, then all others are included
        column_renames = {
            # Core identifiers
            "parcel_id": "parcel_id",
            "PARCELNB": "parcel_number",
            "PIN": "pin",

            # IOS Scoring (priority columns)
            "ios_score": "ios_score",
            "ios_grade": "ios_grade",
            "ios_tier": "ios_tier",
            "ios_notes": "ios_notes",
            "score_parcel_size": "score_parcel_size",
            "score_building_coverage": "score_building_coverage",
            "score_zoning": "score_zoning",
            "score_land_use": "score_land_use",
            "score_structural": "score_structural",
            "score_location": "score_location",

            # Property Address
            "concataddr1": "address",
            "concataddr2": "address_line2",
            "streetno": "street_number",
            "streetdir": "street_direction",
            "streetname": "street_name",
            "streetsuf": "street_suffix",
            "streetpostdir": "street_post_direction",
            "streetalp": "street_alpha",
            "loccity": "city",
            "loczip": "zip",

            # Owner Information
            "ownernamefull": "owner_name",
            "ownername1": "owner_name_1",
            "ownername2": "owner_name_2",
            "owneraddressfull": "owner_address_full",
            "owneraddress": "owner_address",
            "ownercity": "owner_city",
            "ownerstate": "owner_state",
            "ownerzip": "owner_zip",
            "ownerpostalcode": "owner_postal_code",
            "ownercpp": "owner_cpp",
            "ownercsz": "owner_city_state_zip",
            "ownerprovince": "owner_province",
            "ownercountry": "owner_country",

            # Parcel Size & Coverage
            "parcel_area_sqft": "parcel_sqft",
            "lot_size": "lot_size",
            "lot_measure_unit": "lot_measure_unit",
            "building_coverage_pct": "building_coverage_pct",
            "Shape_Area": "shape_area",
            "Shape_Length": "shape_length",

            # Building Information
            "building_footprint_count": "building_count",
            "building_footprint_sqft": "building_footprint_sqft",
            "total_building_sqft": "total_building_sqft",
            "improvement_count": "improvement_count",
            "building_description": "building_description",
            "oldest_year_built": "year_built",
            "bedrooms": "bedrooms",
            "bathrooms": "bathrooms",
            "rooms": "rooms",
            "vacant_improved": "vacant_improved",

            # Zoning
            "zoning_code": "zoning_code",
            "zoning_jurisdiction": "zoning_jurisdiction",

            # Property Type & Use
            "property_type": "property_type",

            # Values
            "actual_total_value": "actual_total_value",
            "actual_land_value": "actual_land_value",
            "actual_improvement_value": "actual_improvement_value",
            "assessed_total_value": "assessed_total_value",

            # Sales History
            "last_sale_date": "last_sale_date",
            "last_sale_price": "last_sale_price",
            "last_deed_type": "last_deed_type",
            "buyer": "buyer",
            "seller": "seller",

            # Legal & Subdivision
            "legal": "legal_description",
            "subname": "subdivision_name",
        }

        result_data = {}
        used_source_cols = set()

        # First, add priority columns in order using the rename map
        for source_col, dest_col in column_renames.items():
            if source_col in df.columns:
                result_data[dest_col] = df[source_col]
                used_source_cols.add(source_col)

        # Add computed columns

        # Calculate acres from sqft if not already present
        if "parcel_sqft" in result_data and "acres" not in result_data:
            result_data["acres"] = result_data["parcel_sqft"] / 43560.0

        # Add latitude/longitude from geometry centroids
        if "geometry" in df.columns:
            try:
                centroids = df.geometry.centroid
                result_data["latitude"] = centroids.y
                result_data["longitude"] = centroids.x
                used_source_cols.add("geometry")
            except Exception as e:
                logger.warning(f"Could not extract centroids: {e}")

        # Add Google Maps link if we have coordinates
        if "latitude" in result_data and "longitude" in result_data:
            result_data["google_maps_url"] = [
                f"https://www.google.com/maps?q={lat},{lon}"
                if pd.notna(lat) and pd.notna(lon)
                else ""
                for lat, lon in zip(result_data["latitude"], result_data["longitude"])
            ]

        # Calculate price per acre
        if "actual_land_value" in result_data and "acres" in result_data:
            acres_series = result_data["acres"]
            land_series = result_data["actual_land_value"]
            result_data["price_per_acre"] = [
                round(land / acres, 0) if pd.notna(land) and pd.notna(acres) and acres > 0 else None
                for land, acres in zip(land_series, acres_series)
            ]

        # Calculate open space percentage
        if "building_coverage_pct" in result_data:
            result_data["open_space_pct"] = [
                100 - cov if pd.notna(cov) else None
                for cov in result_data["building_coverage_pct"]
            ]

        # Add state (always Colorado for Adams County)
        result_data["state"] = "CO"

        # Now add any remaining columns that weren't explicitly mapped
        # (to ensure we don't lose any data)
        for col in df.columns:
            if col not in used_source_cols and col != "geometry":
                # Create a clean column name
                clean_name = col.lower().replace(" ", "_")
                if clean_name not in result_data:
                    result_data[f"raw_{clean_name}"] = df[col]

        return pd.DataFrame(result_data)

    def _find_column(self, df: pd.DataFrame, *options: str) -> Optional[str]:
        """Find the first matching column from a list of options."""
        for opt in options:
            if opt in df.columns:
                return opt
            # Try case-insensitive match
            for col in df.columns:
                if col.lower() == opt.lower():
                    return col
        return None


def export_to_csv(
    scored_df: pd.DataFrame,
    output_dir: Path | str = "deliverables",
    filename: str = "denver_ios_crm.csv",
    min_grade: Optional[str] = None,
) -> Path:
    """
    Convenience function to export scored properties to CSV.

    Args:
        scored_df: DataFrame with IOS scores and property data
        output_dir: Output directory
        filename: Output filename
        min_grade: Minimum grade to include (None for all)

    Returns:
        Path to generated CSV file
    """
    exporter = CSVExporter(output_dir)
    include_all = min_grade is None
    return exporter.export(scored_df, filename, include_all=include_all, min_grade=min_grade)
