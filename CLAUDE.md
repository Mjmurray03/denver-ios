# Denver IOS Property Sourcing System

## Project Overview
Automated Industrial Outdoor Storage (IOS) property sourcing system for the Denver/Commerce City area. This system aggregates property data, calculates building coverage ratios, and scores properties for IOS investment potential.

## Key Technical Concepts

### IOS Scoring (CRITICAL - Inverted Logic)
Unlike traditional CRE analysis, IOS properties need LOW building coverage:
- Optimal coverage: 5-15% (score 100)
- Good coverage: 15-25% (score 85)
- Poor coverage: >40% (score 30 or less)
- We WANT open outdoor space, not buildings

### Technology Stack
- Python 3.13+ with async/await patterns
- PostgreSQL with PostGIS for raw data storage
- MongoDB for processed/scored properties
- geopandas, shapely, rtree for geospatial processing
- httpx for async HTTP requests

### Data Sources
- Adams County GIS REST API (parcels, zoning, assessments)
- Microsoft Building Footprints (building polygons)
- Target area: 1.32 km near Commerce City/DIA

## Development Workflow
1. Use Ref.tools MCP for official documentation lookup
2. Use Exa MCP for current best practices and patterns
3. Research BEFORE coding (Just-in-Time Context pattern)
4. All geospatial work uses EPSG:4326 (WGS84), project to EPSG:26913 for area calculations

## Project Structure
- src/acquisition/ - Data fetching from APIs
- src/processing/ - Validation, building matching, metrics
- src/scoring/ - IOS classification algorithms
- src/storage/ - PostgreSQL and MongoDB operations
- src/filtering/ - Dynamic property filtering
- src/export/ - Excel, CSV, GeoJSON, HTML map generation

## Commands
- Activate venv: .\venv\Scripts\Activate
- Run tests: pytest tests/
- Install deps: pip install -r requirements.txt
