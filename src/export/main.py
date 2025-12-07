"""
Main script to generate all IOS analysis deliverables.

Loads unified dataset, scores properties, and generates:
- Excel workbook with multi-sheet analysis
- Interactive HTML map
- CSV file for CRM import
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.acquisition.file_loader import AdamsCountyFileLoader
from src.export.csv_exporter import export_to_csv
from src.export.excel_exporter import export_to_excel
from src.export.map_generator import generate_map
from src.processing.data_integrator import PropertyDataIntegrator
from src.scoring.ios_scorer import IOSScorer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Generate all IOS analysis deliverables."""
    print("\n" + "=" * 60)
    print("DENVER IOS PROPERTY ANALYSIS - DELIVERABLE GENERATION")
    print("=" * 60 + "\n")

    start_time = datetime.now()
    deliverables_dir = project_root / "deliverables"
    deliverables_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Load and integrate data
    print("Step 1: Loading unified dataset...")
    print("-" * 40)

    loader = AdamsCountyFileLoader()
    integrator = PropertyDataIntegrator(loader)

    unified_df = integrator.create_unified_dataset(
        include_sales=True,
        include_buildings=True,
        include_zoning=True,
    )

    print(f"  Loaded {len(unified_df)} properties")

    # Step 2: Score properties
    print("\nStep 2: Scoring properties for IOS suitability...")
    print("-" * 40)

    scorer = IOSScorer()
    scored_df = scorer.score_dataset(unified_df)

    # Get score distribution
    grade_counts = scored_df["ios_grade"].value_counts().sort_index()
    print("\n  Score Distribution:")
    for grade in ["A", "B", "C", "D", "F"]:
        count = grade_counts.get(grade, 0)
        pct = count / len(scored_df) * 100
        bar = "#" * int(pct / 2)
        print(f"    Grade {grade}: {count:4d} ({pct:5.1f}%) {bar}")

    a_count = grade_counts.get("A", 0)
    b_count = grade_counts.get("B", 0)
    print(f"\n  High-priority candidates (A+B): {a_count + b_count}")

    # Step 3: Generate Excel workbook
    print("\nStep 3: Generating Excel workbook...")
    print("-" * 40)

    filter_criteria = {
        "Area": "Adams County, CO",
        "Data Sources": "Adams County Assessor, GIS, Zoning",
        "Analysis Date": datetime.now().strftime("%Y-%m-%d"),
        "Minimum Parcel Size": "0.5 acres",
        "Target Use": "Industrial Outdoor Storage (IOS)",
    }

    excel_path = export_to_excel(
        scored_df,
        output_dir=deliverables_dir,
        filename="denver_ios_analysis.xlsx",
        filter_criteria=filter_criteria,
    )
    print(f"  [OK] Excel workbook: {excel_path}")
    print(f"    - Executive Summary")
    print(f"    - Top Candidates ({a_count + b_count} A/B grade properties)")
    print(f"    - All Properties ({len(scored_df)} total)")
    print(f"    - Map Data (GIS export)")
    print(f"    - Methodology")

    # Step 4: Generate interactive map (A and B grade only for performance)
    print("\nStep 4: Generating interactive HTML map...")
    print("-" * 40)

    # Filter to A and B grades only for faster map loading
    top_candidates_df = scored_df[scored_df["ios_grade"].isin(["A", "B"])].copy()
    print(f"  Filtering to {len(top_candidates_df)} A/B grade properties for map...")

    map_path = generate_map(
        top_candidates_df,
        output_dir=deliverables_dir,
        filename="denver_ios_map.html",
        use_clustering=True,
    )
    print(f"  [OK] Interactive map: {map_path}")
    print(f"    - {len(top_candidates_df)} A/B grade properties")
    print(f"    - Marker clustering enabled")
    print(f"    - Color-coded by grade (green=A, lightgreen=B)")
    print(f"    - Popups with property details and Google Maps links")

    # Step 5: Generate CRM CSV
    print("\nStep 5: Generating CRM CSV...")
    print("-" * 40)

    csv_path = export_to_csv(
        scored_df,
        output_dir=deliverables_dir,
        filename="denver_ios_crm.csv",
        min_grade=None,  # Include all
    )
    print(f"  [OK] CRM CSV: {csv_path}")
    print(f"    - {len(scored_df)} records")
    print(f"    - Clean column names for CRM import")
    print(f"    - Includes Google Maps URLs")

    # Also generate A/B only CSV
    csv_ab_path = export_to_csv(
        scored_df,
        output_dir=deliverables_dir,
        filename="denver_ios_top_candidates.csv",
        min_grade="B",
    )
    print(f"  [OK] Top candidates CSV: {csv_ab_path}")
    print(f"    - {a_count + b_count} A/B grade records only")

    # Summary
    elapsed = datetime.now() - start_time
    print("\n" + "=" * 60)
    print("DELIVERABLE GENERATION COMPLETE")
    print("=" * 60)
    print(f"\nGenerated files in: {deliverables_dir}")
    print(f"  1. denver_ios_analysis.xlsx   - Full Excel workbook")
    print(f"  2. denver_ios_map.html         - Interactive map")
    print(f"  3. denver_ios_crm.csv          - All properties for CRM")
    print(f"  4. denver_ios_top_candidates.csv - A/B grade only")
    print(f"\nTotal time: {elapsed.total_seconds():.1f} seconds")
    print(f"\nKey Statistics:")
    print(f"  - Total properties analyzed: {len(scored_df)}")
    print(f"  - A-grade candidates: {a_count}")
    print(f"  - B-grade candidates: {b_count}")
    print(f"  - Average IOS score: {scored_df['ios_score'].mean():.1f}")
    print(f"  - Max IOS score: {scored_df['ios_score'].max():.1f}")


if __name__ == "__main__":
    main()
