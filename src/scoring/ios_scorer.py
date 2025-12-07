"""
IOS (Industrial Outdoor Storage) Scoring Engine.

This module implements a 6-dimension weighted scoring system for evaluating
properties for IOS suitability. The scoring dimensions are:

1. Parcel Size (25%) - Optimal size range for IOS operations
2. Building Coverage (30%) - INVERTED: Less coverage = better for IOS
3. Zoning (20%) - Industrial/storage-friendly zoning compatibility
4. Land Use (15%) - Current use classification compatibility
5. Structural (5%) - Building count and size factors
6. Location (5%) - Geographic and access factors

Scores are configurable via config/scoring_weights.yaml.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Conversion constants
SQFT_PER_ACRE = 43560


@dataclass
class ScoreResult:
    """Container for scoring results with component breakdown."""

    composite_score: float
    parcel_size_score: float
    building_coverage_score: float
    zoning_score: float
    land_use_score: float
    structural_score: float
    location_score: float
    grade: str
    tier_label: str
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for DataFrame integration."""
        return {
            "ios_score": self.composite_score,
            "ios_grade": self.grade,
            "ios_tier": self.tier_label,
            "score_parcel_size": self.parcel_size_score,
            "score_building_coverage": self.building_coverage_score,
            "score_zoning": self.zoning_score,
            "score_land_use": self.land_use_score,
            "score_structural": self.structural_score,
            "score_location": self.location_score,
            "ios_notes": "; ".join(self.notes) if self.notes else "",
        }


class IOSScorer:
    """
    6-dimension weighted scoring engine for IOS property evaluation.

    Evaluates properties across multiple dimensions and produces a
    composite score indicating IOS suitability. Higher scores indicate
    better candidates for Industrial Outdoor Storage use.

    Usage:
        scorer = IOSScorer()

        # Score single parcel
        result = scorer.score_parcel(parcel_row)

        # Score entire dataset
        scored_gdf = scorer.score_dataset(unified_gdf)

    Attributes:
        config: Scoring configuration loaded from YAML.
        weights: Dimension weights for composite calculation.
    """

    def __init__(
        self,
        config_path: Optional[Path] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        """
        Initialize the IOS scorer.

        Args:
            config_path: Path to scoring_weights.yaml. Auto-detected if not provided.
            project_root: Project root directory for config lookup.
        """
        if config_path is None:
            if project_root is None:
                project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "scoring_weights.yaml"

        self.config_path = config_path
        self.config = self._load_config()
        self.weights = self.config.get("weights", {})

        logger.info("Initialized IOSScorer")
        logger.info("  Config: %s", self.config_path)
        logger.info("  Weights: %s", self.weights)

    def _load_config(self) -> dict[str, Any]:
        """Load scoring configuration from YAML file."""
        if not self.config_path.exists():
            logger.warning("Config not found at %s, using defaults", self.config_path)
            return self._get_default_config()

        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)

        logger.info("Loaded scoring config from %s", self.config_path)
        return config

    def _get_default_config(self) -> dict[str, Any]:
        """Return default configuration if YAML not found."""
        return {
            "weights": {
                "parcel_size": 0.25,
                "building_coverage": 0.30,
                "zoning": 0.20,
                "land_use": 0.15,
                "structural": 0.05,
                "location": 0.05,
            },
            "classification": {
                "tiers": [
                    {"min": 85, "max": 100, "grade": "A", "label": "Excellent IOS Candidate"},
                    {"min": 75, "max": 84, "grade": "B", "label": "Good IOS Candidate"},
                    {"min": 65, "max": 74, "grade": "C", "label": "Moderate IOS Candidate"},
                    {"min": 50, "max": 64, "grade": "D", "label": "Marginal IOS Candidate"},
                    {"min": 0, "max": 49, "grade": "F", "label": "Poor IOS Candidate"},
                ]
            },
        }

    def _get_value(self, row: pd.Series, *columns: str) -> Any:
        """Get first available value from multiple possible column names."""
        for col in columns:
            if col in row.index and pd.notna(row[col]):
                return row[col]
        return None

    def _score_parcel_size(self, row: pd.Series) -> tuple[float, list[str]]:
        """
        Score parcel based on acreage.

        Args:
            row: DataFrame row with parcel data.

        Returns:
            Tuple of (score, notes list).
        """
        notes = []

        # Get parcel area - try multiple sources
        area_sqft = self._get_value(
            row,
            "parcel_area_sqft",
            "lot_size",
            "Shape_Area",
        )

        if area_sqft is None or area_sqft <= 0:
            notes.append("No parcel area data")
            return 0, notes

        # Convert to acres
        # Check if lot_size is already in acres (small numbers)
        if area_sqft < 1000:
            # Assume it's already in acres
            acres = float(area_sqft)
        else:
            acres = float(area_sqft) / SQFT_PER_ACRE

        # Score based on thresholds
        thresholds = self.config.get("parcel_size", {}).get("thresholds", [])

        score = 0
        label = "unknown"

        for threshold in thresholds:
            max_val = threshold.get("max")
            if max_val is None or acres <= max_val:
                score = threshold.get("score", 0)
                label = threshold.get("label", "")
                break

        notes.append(f"Parcel size: {acres:.2f} acres ({label})")

        return score, notes

    def _score_building_coverage(self, row: pd.Series) -> tuple[float, list[str]]:
        """
        Score based on building coverage percentage.

        INVERTED LOGIC: Lower coverage = higher score for IOS.
        5-15% coverage is optimal (some structures, mostly open land).

        Args:
            row: DataFrame row with parcel data.

        Returns:
            Tuple of (score, notes list).
        """
        notes = []

        # Get coverage percentage
        coverage_pct = self._get_value(
            row,
            "building_coverage_pct",
        )

        if coverage_pct is None:
            # Try to calculate from building footprint and parcel area
            footprint_sqft = self._get_value(row, "building_footprint_sqft")
            parcel_sqft = self._get_value(row, "parcel_area_sqft", "Shape_Area")

            if footprint_sqft is not None and parcel_sqft is not None and parcel_sqft > 0:
                coverage_pct = (float(footprint_sqft) / float(parcel_sqft)) * 100
            else:
                coverage_pct = 0
                notes.append("No building coverage data - assumed 0%")

        # Score based on thresholds (inverted - lower is better)
        thresholds = self.config.get("building_coverage", {}).get("thresholds", [])

        score = 0
        label = "unknown"

        for threshold in thresholds:
            max_val = threshold.get("max")
            if max_val is None or coverage_pct <= max_val:
                score = threshold.get("score", 0)
                label = threshold.get("label", "")
                break

        notes.append(f"Building coverage: {coverage_pct:.1f}% ({label})")

        return score, notes

    def _score_zoning(self, row: pd.Series) -> tuple[float, list[str]]:
        """
        Score based on zoning classification.

        Args:
            row: DataFrame row with parcel data.

        Returns:
            Tuple of (score, notes list).
        """
        notes = []
        zoning_config = self.config.get("zoning", {})

        # Get zoning code
        zoning_code = self._get_value(
            row,
            "zoning_code",
            "zoning",
            "zone",
            "ZONE",
            "ZONING",
        )

        # Get zoning description
        zoning_desc = self._get_value(
            row,
            "zoning_desc",
            "zoning_description",
            "ZONE_DESC",
        )

        if zoning_code is None:
            notes.append("No zoning data available")
            return zoning_config.get("default_score", 40), notes

        zoning_code_upper = str(zoning_code).upper().strip()
        score = zoning_config.get("default_score", 40)
        matched = False

        # Define zone category checks in priority order
        zone_categories = [
            ("high_value", "codes", "industrial (score 100)"),
            ("medium_high", "codes", "heavy commercial (score 75)"),
            ("moderate", "codes", "highway commercial (score 65)"),
            ("medium", "codes", "agricultural/general commercial (score 55)"),
            ("pud", "codes", "planned development (score 50)"),
            ("low_medium", "codes", "office/community commercial (score 40)"),
        ]

        # Check code-based categories
        for category, list_type, description in zone_categories:
            if matched:
                break
            category_config = zoning_config.get(category, {})
            for code in category_config.get(list_type, []):
                if code.upper() == zoning_code_upper or zoning_code_upper.startswith(code.upper()):
                    score = category_config.get("score", 40)
                    notes.append(f"Zoning: {zoning_code} ({description})")
                    matched = True
                    break

        # Check pattern-based categories (low value and city placeholders)
        if not matched:
            low_value = zoning_config.get("low_value", {})
            for pattern in low_value.get("patterns", []):
                if zoning_code_upper.startswith(pattern.upper()) or pattern.upper() == zoning_code_upper:
                    score = low_value.get("score", 10)
                    notes.append(f"Zoning: {zoning_code} (residential/low-value, score 10)")
                    matched = True
                    break

        # Check city placeholder patterns
        if not matched:
            city_placeholders = zoning_config.get("city_placeholders", {})
            for pattern in city_placeholders.get("patterns", []):
                if pattern.upper() in zoning_code_upper or zoning_code_upper == pattern.upper():
                    score = city_placeholders.get("score", 40)
                    notes.append(f"Zoning: {zoning_code} (city jurisdiction, score 40)")
                    matched = True
                    break

        if not matched:
            notes.append(f"Zoning: {zoning_code} (unrecognized, default score {score})")

        # Check for bonus keywords in description
        if zoning_desc:
            zoning_desc_lower = str(zoning_desc).lower()
            bonus_keywords = zoning_config.get("bonus_keywords", [])

            for kw in bonus_keywords:
                pattern = kw.get("pattern", "").lower()
                bonus = kw.get("bonus", 0)

                if pattern and pattern in zoning_desc_lower:
                    score = min(100, score + bonus)
                    notes.append(f"Zoning keyword bonus: '{pattern}' (+{bonus})")

        return score, notes

    def _score_land_use(self, row: pd.Series) -> tuple[float, list[str]]:
        """
        Score based on current land use and property type.

        Args:
            row: DataFrame row with parcel data.

        Returns:
            Tuple of (score, notes list).
        """
        notes = []
        land_use_config = self.config.get("land_use", {})
        default_score = land_use_config.get("default_score", 50)

        # Gather text fields to search
        text_fields = []

        for field in [
            "land_use",
            "land_use_desc",
            "property_type",
            "building_description",
            "occupancy_description",
            "use_code",
            "use_desc",
            "proptype",
            "bltasdesc",
        ]:
            val = self._get_value(row, field)
            if val:
                text_fields.append(str(val).lower())

        combined_text = " ".join(text_fields)

        if not combined_text.strip():
            notes.append("No land use/property type data")
            return default_score, notes

        score = default_score
        matched_keyword = None

        # Check high value keywords
        high_value_keywords = land_use_config.get("high_value_keywords", [])
        for kw in high_value_keywords:
            pattern = kw.get("pattern", "").lower()
            if pattern and pattern in combined_text:
                kw_score = kw.get("score", 100)
                if kw_score > score:
                    score = kw_score
                    matched_keyword = pattern

        # Check moderate keywords (only if no high value match)
        if matched_keyword is None:
            moderate_keywords = land_use_config.get("moderate_keywords", [])
            for kw in moderate_keywords:
                pattern = kw.get("pattern", "").lower()
                if pattern and pattern in combined_text:
                    kw_score = kw.get("score", 60)
                    if kw_score > score:
                        score = kw_score
                        matched_keyword = pattern

        # Check property type patterns
        property_types = land_use_config.get("property_types", [])
        for pt in property_types:
            pattern = pt.get("pattern", "").lower()
            if pattern and pattern in combined_text:
                pt_score = pt.get("score", 50)
                # Use higher of keyword match and property type
                if pt_score > score:
                    score = pt_score
                    matched_keyword = f"property type: {pattern}"

        if matched_keyword:
            notes.append(f"Land use match: '{matched_keyword}'")
        else:
            notes.append("Land use: no specific IOS keywords found")

        return score, notes

    def _score_structural(self, row: pd.Series) -> tuple[float, list[str]]:
        """
        Score based on building count and sizes.

        Args:
            row: DataFrame row with parcel data.

        Returns:
            Tuple of (score, notes list).
        """
        notes = []
        structural_config = self.config.get("structural", {})
        base_score = structural_config.get("base_score", 50)

        # Get building count
        building_count = self._get_value(
            row,
            "building_footprint_count",
            "improvement_count",
            "num_buildings",
        )

        if building_count is None:
            building_count = 0

        building_count = int(building_count)

        # Get largest building size
        largest_building = self._get_value(
            row,
            "total_building_sqft",
            "building_sqft",
            "building_footprint_sqft",
        )

        if largest_building is None:
            largest_building = 0

        largest_building = float(largest_building)

        # Calculate building count adjustment
        count_adjustment = 0
        count_label = ""
        count_thresholds = structural_config.get("building_count", [])

        for threshold in count_thresholds:
            count_val = threshold.get("count")
            if count_val is None or building_count <= count_val:
                count_adjustment = threshold.get("adjustment", 0)
                count_label = threshold.get("label", "")
                break

        # Calculate building size adjustment
        size_adjustment = 0
        size_label = ""
        size_thresholds = structural_config.get("building_size", [])

        for threshold in size_thresholds:
            max_val = threshold.get("max")
            if max_val is None or largest_building <= max_val:
                size_adjustment = threshold.get("adjustment", 0)
                size_label = threshold.get("label", "")
                break

        # Calculate final score
        score = base_score + count_adjustment + size_adjustment
        score = max(0, min(100, score))  # Clamp to 0-100

        notes.append(f"Buildings: {building_count} ({count_label}, {count_adjustment:+d})")

        if largest_building > 0:
            notes.append(f"Largest building: {largest_building:,.0f} sqft ({size_label}, {size_adjustment:+d})")
        else:
            notes.append("No building footprint data")

        return score, notes

    def _score_location(self, row: pd.Series) -> tuple[float, list[str]]:
        """
        Score based on location factors.

        Args:
            row: DataFrame row with parcel data.

        Returns:
            Tuple of (score, notes list).
        """
        notes = []
        location_config = self.config.get("location", {})

        # Start with base score for being in target area
        score = location_config.get("base_score", 60)
        notes.append("In target industrial area")

        # Add DIA proximity bonus (entire Commerce City area qualifies)
        dia_bonus = location_config.get("dia_proximity_bonus", 15)
        score += dia_bonus
        notes.append(f"DIA proximity bonus (+{dia_bonus})")

        # Future: highway access scoring
        highway_config = location_config.get("highway_access", {})
        if highway_config.get("enabled", False):
            # Would check proximity to I-70, I-270, I-76
            pass

        # Future: residential proximity penalty
        residential_config = location_config.get("residential_proximity", {})
        if residential_config.get("enabled", False):
            # Would check distance to residential zones
            pass

        score = max(0, min(100, score))

        return score, notes

    def classify_confidence(self, score: float) -> tuple[str, str]:
        """
        Classify a composite score into a tier.

        Args:
            score: Composite IOS score (0-100).

        Returns:
            Tuple of (grade letter, tier label).
        """
        tiers = self.config.get("classification", {}).get("tiers", [])

        for tier in tiers:
            min_val = tier.get("min", 0)
            max_val = tier.get("max", 100)

            if min_val <= score <= max_val:
                return tier.get("grade", "?"), tier.get("label", "Unknown")

        return "F", "Poor IOS Candidate"

    def generate_analysis_notes(
        self,
        row: pd.Series,
        scores: dict[str, float],
        all_notes: list[str],
    ) -> str:
        """
        Generate human-readable analysis notes.

        Args:
            row: Original data row.
            scores: Component scores dictionary.
            all_notes: Notes collected during scoring.

        Returns:
            Formatted analysis string.
        """
        composite = scores.get("composite", 0)
        grade, tier_label = self.classify_confidence(composite)

        lines = [
            f"IOS Score: {composite:.1f} ({grade} - {tier_label})",
            "",
            "Component Scores:",
            f"  Parcel Size:      {scores.get('parcel_size', 0):.0f}/100 (weight: 25%)",
            f"  Building Coverage: {scores.get('building_coverage', 0):.0f}/100 (weight: 30%)",
            f"  Zoning:           {scores.get('zoning', 0):.0f}/100 (weight: 20%)",
            f"  Land Use:         {scores.get('land_use', 0):.0f}/100 (weight: 15%)",
            f"  Structural:       {scores.get('structural', 0):.0f}/100 (weight: 5%)",
            f"  Location:         {scores.get('location', 0):.0f}/100 (weight: 5%)",
            "",
            "Analysis Notes:",
        ]

        for note in all_notes:
            lines.append(f"  - {note}")

        return "\n".join(lines)

    def score_parcel(self, row: pd.Series) -> ScoreResult:
        """
        Score a single parcel for IOS suitability.

        Args:
            row: DataFrame row containing parcel data.

        Returns:
            ScoreResult with composite and component scores.
        """
        all_notes = []

        # Calculate each dimension score
        parcel_size_score, size_notes = self._score_parcel_size(row)
        all_notes.extend(size_notes)

        coverage_score, coverage_notes = self._score_building_coverage(row)
        all_notes.extend(coverage_notes)

        zoning_score, zoning_notes = self._score_zoning(row)
        all_notes.extend(zoning_notes)

        land_use_score, land_use_notes = self._score_land_use(row)
        all_notes.extend(land_use_notes)

        structural_score, structural_notes = self._score_structural(row)
        all_notes.extend(structural_notes)

        location_score, location_notes = self._score_location(row)
        all_notes.extend(location_notes)

        # Calculate weighted composite score
        composite = (
            parcel_size_score * self.weights.get("parcel_size", 0.25)
            + coverage_score * self.weights.get("building_coverage", 0.30)
            + zoning_score * self.weights.get("zoning", 0.20)
            + land_use_score * self.weights.get("land_use", 0.15)
            + structural_score * self.weights.get("structural", 0.05)
            + location_score * self.weights.get("location", 0.05)
        )

        # Classify
        grade, tier_label = self.classify_confidence(composite)

        return ScoreResult(
            composite_score=composite,
            parcel_size_score=parcel_size_score,
            building_coverage_score=coverage_score,
            zoning_score=zoning_score,
            land_use_score=land_use_score,
            structural_score=structural_score,
            location_score=location_score,
            grade=grade,
            tier_label=tier_label,
            notes=all_notes,
        )

    def score_dataset(
        self,
        gdf: gpd.GeoDataFrame,
        add_notes: bool = True,
    ) -> gpd.GeoDataFrame:
        """
        Score an entire GeoDataFrame of parcels.

        Args:
            gdf: GeoDataFrame with parcel data.
            add_notes: Whether to include analysis notes column.

        Returns:
            GeoDataFrame with score columns added.
        """
        logger.info("Scoring %d parcels...", len(gdf))

        # Score each parcel
        results = []
        for idx, row in gdf.iterrows():
            result = self.score_parcel(row)
            results.append(result.to_dict())

        # Create results DataFrame
        results_df = pd.DataFrame(results)

        # Drop notes column if not requested
        if not add_notes and "ios_notes" in results_df.columns:
            results_df = results_df.drop(columns=["ios_notes"])

        # Merge with original GeoDataFrame
        scored_gdf = gdf.copy()
        for col in results_df.columns:
            scored_gdf[col] = results_df[col].values

        # Sort by IOS score descending
        scored_gdf = scored_gdf.sort_values("ios_score", ascending=False)

        logger.info("Scoring complete")
        logger.info("  Score distribution:")
        logger.info("    Mean: %.1f", scored_gdf["ios_score"].mean())
        logger.info("    Median: %.1f", scored_gdf["ios_score"].median())
        logger.info("    Min: %.1f", scored_gdf["ios_score"].min())
        logger.info("    Max: %.1f", scored_gdf["ios_score"].max())

        # Log grade distribution
        grade_counts = scored_gdf["ios_grade"].value_counts().sort_index()
        logger.info("  Grade distribution:")
        for grade, count in grade_counts.items():
            pct = count / len(scored_gdf) * 100
            logger.info("    %s: %d (%.1f%%)", grade, count, pct)

        return scored_gdf


def main() -> None:
    """
    Test the IOS scorer with unified dataset.
    """
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("\n" + "=" * 70)
    print("IOS SCORING ENGINE TEST")
    print("=" * 70)

    try:
        # Import integrator to get unified dataset
        from src.acquisition import TargetArea
        from src.processing import PropertyDataIntegrator

        # Step 1: Load unified dataset
        print("\n[1] Loading Unified Dataset")
        print("-" * 50)

        integrator = PropertyDataIntegrator()
        target = TargetArea()
        boundary = target.get_bounding_box()

        print(f"Target area: ({target.center_latitude}, {target.center_longitude})")
        print(f"Radius: {target.radius_km} km")

        unified = integrator.create_unified_dataset(
            boundary=boundary,
            include_sales=True,
            include_buildings=True,
        )

        print(f"Loaded {len(unified)} parcels")

        # Step 2: Initialize scorer
        print("\n[2] Initializing IOS Scorer")
        print("-" * 50)

        scorer = IOSScorer()

        # Step 3: Score all parcels
        print("\n[3] Scoring Parcels")
        print("-" * 50)

        scored = scorer.score_dataset(unified, add_notes=True)

        # Step 4: Score distribution
        print("\n[4] Score Distribution")
        print("-" * 50)

        print(f"\nOverall Statistics:")
        print(f"  Total parcels: {len(scored):,}")
        print(f"  Mean score: {scored['ios_score'].mean():.1f}")
        print(f"  Median score: {scored['ios_score'].median():.1f}")
        print(f"  Std dev: {scored['ios_score'].std():.1f}")
        print(f"  Min: {scored['ios_score'].min():.1f}")
        print(f"  Max: {scored['ios_score'].max():.1f}")

        print(f"\nGrade Distribution:")
        grade_counts = scored["ios_grade"].value_counts().sort_index()
        for grade, count in grade_counts.items():
            pct = count / len(scored) * 100
            bar = "#" * int(pct / 2)
            print(f"  {grade}: {count:5,} ({pct:5.1f}%) {bar}")

        print(f"\nTier Distribution:")
        tier_counts = scored["ios_tier"].value_counts()
        for tier, count in tier_counts.items():
            pct = count / len(scored) * 100
            print(f"  {tier}: {count:,} ({pct:.1f}%)")

        # Step 5: Top 10 candidates
        print("\n[5] Top 10 IOS Candidates")
        print("-" * 50)

        top_10 = scored.head(10)

        for i, (idx, row) in enumerate(top_10.iterrows(), 1):
            print(f"\n{'='*60}")
            print(f"RANK #{i}")
            print(f"{'='*60}")

            # Basic info
            parcel_id = row.get("parcel_id", "Unknown")
            address = row.get("concataddr1", "No address")

            print(f"Parcel ID: {parcel_id}")
            print(f"Address: {address}")

            # Score breakdown
            print(f"\nIOS Score: {row['ios_score']:.1f} ({row['ios_grade']} - {row['ios_tier']})")
            print(f"\nComponent Scores:")
            print(f"  Parcel Size:       {row['score_parcel_size']:5.0f}/100 (25%)")
            print(f"  Building Coverage: {row['score_building_coverage']:5.0f}/100 (30%)")
            print(f"  Zoning:            {row['score_zoning']:5.0f}/100 (20%)")
            print(f"  Land Use:          {row['score_land_use']:5.0f}/100 (15%)")
            print(f"  Structural:        {row['score_structural']:5.0f}/100 (5%)")
            print(f"  Location:          {row['score_location']:5.0f}/100 (5%)")

            # Key metrics
            print(f"\nKey Metrics:")

            parcel_sqft = row.get("parcel_area_sqft", 0)
            if parcel_sqft and parcel_sqft > 0:
                acres = parcel_sqft / SQFT_PER_ACRE
                print(f"  Parcel Size: {acres:.2f} acres ({parcel_sqft:,.0f} sqft)")

            coverage = row.get("building_coverage_pct", 0)
            print(f"  Building Coverage: {coverage:.1f}%")

            value = row.get("actual_total_value", 0)
            if value:
                print(f"  Assessed Value: ${value:,.0f}")

            # Analysis notes
            notes = row.get("ios_notes", "")
            if notes:
                print(f"\nAnalysis Notes:")
                for note in notes.split("; "):
                    print(f"  - {note}")

        # Step 6: Summary of A and B grade candidates
        print("\n[6] High-Priority Candidates Summary")
        print("-" * 50)

        a_grade = scored[scored["ios_grade"] == "A"]
        b_grade = scored[scored["ios_grade"] == "B"]

        print(f"\nA-Grade Candidates: {len(a_grade)}")
        if len(a_grade) > 0:
            print(f"  Score range: {a_grade['ios_score'].min():.1f} - {a_grade['ios_score'].max():.1f}")
            print(f"  Avg parcel size: {a_grade['parcel_area_sqft'].mean() / SQFT_PER_ACRE:.2f} acres")
            print(f"  Avg coverage: {a_grade['building_coverage_pct'].mean():.1f}%")

        print(f"\nB-Grade Candidates: {len(b_grade)}")
        if len(b_grade) > 0:
            print(f"  Score range: {b_grade['ios_score'].min():.1f} - {b_grade['ios_score'].max():.1f}")
            print(f"  Avg parcel size: {b_grade['parcel_area_sqft'].mean() / SQFT_PER_ACRE:.2f} acres")
            print(f"  Avg coverage: {b_grade['building_coverage_pct'].mean():.1f}%")

        print(f"\nTotal High-Priority (A+B): {len(a_grade) + len(b_grade)} parcels")

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
