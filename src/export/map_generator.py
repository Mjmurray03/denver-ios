"""
Interactive Map Generator for IOS Analysis.

Generates HTML maps using Folium with marker clusters and
grade-based color coding for property visualization.
"""

import logging
from pathlib import Path
from typing import Optional

import folium
import pandas as pd
from folium.plugins import MarkerCluster

logger = logging.getLogger(__name__)

# Default map center (Commerce City / DIA area)
DEFAULT_CENTER = (39.82026, -104.90811)
DEFAULT_ZOOM = 11

# Marker colors by grade
GRADE_COLORS = {
    "A": "green",
    "B": "lightgreen",
    "C": "orange",
    "D": "beige",
    "F": "red",
}

# Marker icons by grade
GRADE_ICONS = {
    "A": "star",
    "B": "ok-sign",
    "C": "info-sign",
    "D": "question-sign",
    "F": "remove-sign",
}


class MapGenerator:
    """Generate interactive HTML maps for IOS property analysis."""

    def __init__(
        self,
        output_dir: Path | str = "deliverables",
        center: tuple[float, float] = DEFAULT_CENTER,
        zoom: int = DEFAULT_ZOOM,
    ):
        """
        Initialize the map generator.

        Args:
            output_dir: Directory for output files
            center: Map center coordinates (lat, lon)
            zoom: Initial zoom level
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.center = center
        self.zoom = zoom

    def generate(
        self,
        scored_df: pd.DataFrame,
        filename: str = "denver_ios_map.html",
        use_clustering: bool = True,
        max_markers: Optional[int] = None,
    ) -> Path:
        """
        Generate an interactive HTML map of scored properties.

        Args:
            scored_df: DataFrame with IOS scores and property data
            filename: Output filename
            use_clustering: Whether to use marker clustering
            max_markers: Maximum number of markers to display (None for all)

        Returns:
            Path to the generated HTML file
        """
        output_path = self.output_dir / filename
        logger.info(f"Generating interactive map: {output_path}")

        # Create base map
        m = folium.Map(
            location=self.center,
            zoom_start=self.zoom,
            tiles="OpenStreetMap",
        )

        # Add alternative tile layers
        folium.TileLayer("cartodbpositron", name="Light").add_to(m)
        folium.TileLayer("cartodbdark_matter", name="Dark").add_to(m)

        # Add satellite imagery layers
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            attr="Esri",
            name="Satellite",
        ).add_to(m)

        folium.TileLayer(
            tiles="https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
            attr="Google",
            name="Google Satellite",
        ).add_to(m)

        folium.TileLayer(
            tiles="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}",
            attr="Google",
            name="Google Hybrid",
        ).add_to(m)

        # Prepare data
        df = scored_df.copy()

        # Get coordinate columns
        lat_col = self._find_column(df, "lat", "latitude", "centroid_lat")
        lon_col = self._find_column(df, "lon", "longitude", "lng", "centroid_lon")

        # If no lat/lon columns, try to extract from geometry
        if (not lat_col or not lon_col) and "geometry" in df.columns:
            logger.info("Extracting coordinates from geometry centroids...")
            try:
                # Get centroids of geometries
                centroids = df.geometry.centroid
                df["_map_lat"] = centroids.y
                df["_map_lon"] = centroids.x
                lat_col = "_map_lat"
                lon_col = "_map_lon"
                logger.info(f"Extracted {len(df)} centroids from geometry")
            except Exception as e:
                logger.error(f"Failed to extract centroids: {e}")

        if not lat_col or not lon_col:
            logger.error("Could not find latitude/longitude columns in data")
            # Create empty map
            m.save(output_path)
            return output_path

        # Filter to valid coordinates
        valid_coords = df[
            df[lat_col].notna()
            & df[lon_col].notna()
            & (df[lat_col] != 0)
            & (df[lon_col] != 0)
        ].copy()

        logger.info(f"Properties with valid coordinates: {len(valid_coords)}")

        # Limit markers if specified
        if max_markers and len(valid_coords) > max_markers:
            # Prioritize higher scores
            score_col = self._find_column(valid_coords, "ios_score", "score")
            if score_col:
                valid_coords = valid_coords.nlargest(max_markers, score_col)
            else:
                valid_coords = valid_coords.head(max_markers)

        # Create marker cluster or feature groups by grade
        if use_clustering:
            marker_cluster = MarkerCluster(name="All Properties")
            marker_cluster.add_to(m)
            marker_container = marker_cluster
        else:
            marker_container = m

        # Add markers
        grade_col = self._find_column(valid_coords, "ios_grade", "grade")
        score_col = self._find_column(valid_coords, "ios_score", "score")

        markers_added = 0
        for _, row in valid_coords.iterrows():
            try:
                lat = float(row[lat_col])
                lon = float(row[lon_col])

                # Skip invalid coordinates
                if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                    continue

                # Get grade and color
                grade = row[grade_col] if grade_col and pd.notna(row[grade_col]) else "C"
                color = GRADE_COLORS.get(grade, "gray")
                icon = GRADE_ICONS.get(grade, "info-sign")

                # Build popup content
                popup_html = self._build_popup(row, lat, lon)

                # Create marker
                marker = folium.Marker(
                    location=[lat, lon],
                    popup=folium.Popup(popup_html, max_width=350),
                    icon=folium.Icon(color=color, icon=icon, prefix="glyphicon"),
                    tooltip=self._build_tooltip(row),
                )

                marker.add_to(marker_container)
                markers_added += 1

            except (ValueError, TypeError) as e:
                logger.debug(f"Skipping row due to error: {e}")
                continue

        logger.info(f"Added {markers_added} markers to map")

        # Add legend
        self._add_legend(m)

        # Add layer control
        folium.LayerControl().add_to(m)

        # Save map
        m.save(output_path)
        logger.info(f"Map saved: {output_path}")

        return output_path

    def _build_popup(self, row: pd.Series, lat: float, lon: float) -> str:
        """Build HTML popup content for a marker."""
        # Get values with fallbacks - check both raw source columns and cleaned export columns
        address = self._get_value(row, "address", "concataddr1", "situs_address", "full_address") or "Unknown"
        parcel_id = self._get_value(row, "parcel_id", "PARCELNB", "accountno", "parcelid") or "N/A"
        score = self._get_value(row, "ios_score", "score")
        grade = self._get_value(row, "ios_grade", "grade") or "N/A"

        # Calculate acres from parcel_area_sqft if acres not directly available
        acres = self._get_value(row, "acres", "calc_acreage", "parcel_acres")
        if acres is None:
            parcel_sqft = self._get_value(row, "parcel_area_sqft", "parcel_sqft", "Shape_Area")
            if parcel_sqft:
                acres = parcel_sqft / 43560.0

        coverage = self._get_value(row, "building_coverage_pct", "coverage_pct")
        zoning = self._get_value(row, "zoning_code", "zoning", "zone") or "N/A"
        owner = self._get_value(row, "owner_name", "ownernamefull", "ownername1", "owner", "owner1") or "N/A"
        assessed = self._get_value(row, "assessed_total_value", "total_assessed", "assessed_value", "total_value")

        # Additional useful fields
        city = self._get_value(row, "city", "loccity") or ""
        actual_value = self._get_value(row, "actual_total_value")
        last_sale_price = self._get_value(row, "last_sale_price")
        last_sale_date = self._get_value(row, "last_sale_date")
        year_built = self._get_value(row, "year_built", "oldest_year_built")
        property_type = self._get_value(row, "property_type")

        # Format values
        score_str = f"{score:.1f}" if score else "N/A"
        acres_str = f"{acres:.2f}" if acres else "N/A"
        coverage_str = f"{coverage:.1f}%" if coverage else "N/A"
        assessed_str = f"${assessed:,.0f}" if assessed else "N/A"
        actual_str = f"${actual_value:,.0f}" if actual_value else "N/A"
        sale_price_str = f"${last_sale_price:,.0f}" if last_sale_price else "N/A"
        sale_date_str = str(last_sale_date)[:10] if last_sale_date else "N/A"
        year_built_str = str(int(year_built)) if year_built else "N/A"

        # Google Maps link
        gmaps_link = f"https://www.google.com/maps?q={lat},{lon}"

        # Grade color
        grade_colors = {
            "A": "#28a745",
            "B": "#5cb85c",
            "C": "#ffc107",
            "D": "#fd7e14",
            "F": "#dc3545",
        }
        grade_color = grade_colors.get(grade, "#6c757d")

        # Format address with city
        full_address = f"{address}, {city}" if city else address

        popup_html = f"""
        <div style="font-family: Arial, sans-serif; min-width: 320px;">
            <h4 style="margin: 0 0 10px 0; color: #333;">{full_address}</h4>
            <div style="background: {grade_color}; color: white; padding: 8px; border-radius: 4px; margin-bottom: 10px; text-align: center;">
                <strong>IOS Score: {score_str}</strong> | <strong>Grade: {grade}</strong>
            </div>
            <table style="width: 100%; font-size: 12px; border-collapse: collapse;">
                <tr><td style="padding: 3px; font-weight: bold;">Parcel ID:</td><td style="padding: 3px;">{parcel_id}</td></tr>
                <tr><td style="padding: 3px; font-weight: bold;">Acres:</td><td style="padding: 3px;">{acres_str}</td></tr>
                <tr><td style="padding: 3px; font-weight: bold;">Building Coverage:</td><td style="padding: 3px;">{coverage_str}</td></tr>
                <tr><td style="padding: 3px; font-weight: bold;">Zoning:</td><td style="padding: 3px;">{zoning}</td></tr>
                <tr><td style="padding: 3px; font-weight: bold;">Owner:</td><td style="padding: 3px;">{owner}</td></tr>
                <tr><td style="padding: 3px; font-weight: bold;">Property Type:</td><td style="padding: 3px;">{property_type or 'N/A'}</td></tr>
                <tr><td style="padding: 3px; font-weight: bold;">Year Built:</td><td style="padding: 3px;">{year_built_str}</td></tr>
                <tr><td style="padding: 3px; font-weight: bold;">Actual Value:</td><td style="padding: 3px;">{actual_str}</td></tr>
                <tr><td style="padding: 3px; font-weight: bold;">Assessed Value:</td><td style="padding: 3px;">{assessed_str}</td></tr>
                <tr><td style="padding: 3px; font-weight: bold;">Last Sale:</td><td style="padding: 3px;">{sale_price_str} ({sale_date_str})</td></tr>
            </table>
            <div style="margin-top: 10px; text-align: center;">
                <a href="{gmaps_link}" target="_blank" style="color: #007bff; text-decoration: none;">
                    Open in Google Maps
                </a>
            </div>
        </div>
        """

        return popup_html

    def _build_tooltip(self, row: pd.Series) -> str:
        """Build tooltip text for hover."""
        address = self._get_value(row, "address", "concataddr1", "situs_address", "full_address") or "Unknown"
        city = self._get_value(row, "city", "loccity") or ""
        score = self._get_value(row, "ios_score", "score")
        grade = self._get_value(row, "ios_grade", "grade")

        score_str = f"{score:.1f}" if score else "N/A"
        grade_str = f" ({grade})" if grade else ""
        full_address = f"{address}, {city}" if city else address

        return f"{full_address} - Score: {score_str}{grade_str}"

    def _add_legend(self, m: folium.Map) -> None:
        """Add a legend to the map."""
        legend_html = """
        <div style="
            position: fixed;
            bottom: 50px;
            left: 50px;
            z-index: 1000;
            background-color: white;
            padding: 10px;
            border: 2px solid gray;
            border-radius: 5px;
            font-family: Arial, sans-serif;
            font-size: 12px;
        ">
            <div style="font-weight: bold; margin-bottom: 5px;">IOS Grade Legend</div>
            <div><span style="background-color: #28a745; color: white; padding: 2px 8px; border-radius: 3px;">A</span> Excellent (85-100)</div>
            <div style="margin-top: 3px;"><span style="background-color: #5cb85c; color: white; padding: 2px 8px; border-radius: 3px;">B</span> Good (75-84)</div>
            <div style="margin-top: 3px;"><span style="background-color: #ffc107; color: black; padding: 2px 8px; border-radius: 3px;">C</span> Moderate (65-74)</div>
            <div style="margin-top: 3px;"><span style="background-color: #fd7e14; color: white; padding: 2px 8px; border-radius: 3px;">D</span> Marginal (50-64)</div>
            <div style="margin-top: 3px;"><span style="background-color: #dc3545; color: white; padding: 2px 8px; border-radius: 3px;">F</span> Poor (0-49)</div>
        </div>
        """
        m.get_root().html.add_child(folium.Element(legend_html))

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

    def _get_value(self, row: pd.Series, *column_options: str):
        """Get value from row, trying multiple column names."""
        for col in column_options:
            if col in row.index and pd.notna(row[col]):
                return row[col]
            # Try case-insensitive
            for idx in row.index:
                if idx.lower() == col.lower() and pd.notna(row[idx]):
                    return row[idx]
        return None


def generate_map(
    scored_df: pd.DataFrame,
    output_dir: Path | str = "deliverables",
    filename: str = "denver_ios_map.html",
    center: tuple[float, float] = DEFAULT_CENTER,
    use_clustering: bool = True,
) -> Path:
    """
    Convenience function to generate an interactive map.

    Args:
        scored_df: DataFrame with IOS scores and property data
        output_dir: Output directory
        filename: Output filename
        center: Map center coordinates
        use_clustering: Whether to use marker clustering

    Returns:
        Path to generated HTML file
    """
    generator = MapGenerator(output_dir, center=center)
    return generator.generate(scored_df, filename, use_clustering=use_clustering)
