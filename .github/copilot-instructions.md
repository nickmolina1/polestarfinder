# Copilot Instructions for polestarfinder

## Project Overview
- **Purpose:** Inventory, filter, and present Polestar vehicle data from multiple sources (scraping, database, static JSON, and web UI).
- **Architecture:**
  - **Backend:** Python ETL/scraper jobs (`jobs/`, `scraper/`), database access (`database/`), migration scripts, and filter logic.
  - **Frontend:** Static site in `public/` (HTML, CSS, JS) consuming `data/vehicles.json` (for local dev) or API (for prod).
  - **Data:** Vehicle records as arrays (see `public/data/vehicles.json`), mapped to objects using `VEHICLE_KEYS` in JS.
  - **Filters:** Centralized in `scraper/filters.py` (maps human labels to codes for color, motor, interior, wheels, packages, etc.).

## Key Patterns & Conventions
- **Image URL Parsing:** Vehicle features (color, motor, interior, wheels, packages) are often encoded in image URL path segments. See `scraper/code_parser.py` for extraction/classification logic.
- **Feature Enrichment:** Use code parsing first; fallback to deep scan (per-feature API calls) only for missing attributes.
- **Database Access:** Use context-managed connections in `database/db.py` (`conn()` yields psycopg2 connection). Logging is set up for connection events.
- **ETL/Jobs:** Main refresh logic in `jobs/daily_refresh.py`. Scraper logic in `scraper/scraper.py`. Jobs update DB and optionally static JSON for frontend.
- **Frontend Data Flow:** JS (`public/app.js`) loads vehicles from JSON, maps arrays to objects, applies filters/sorting, and renders responsive cards. Filters are read from DOM and mapped to codes using `VEHICLE_KEYS` and filter logic.
- **Deployment:** S3 + CloudFront for frontend. `.github/workflows/deploy-frontend.yml` syncs `public/` but excludes `public/data/` from S3. Data file is for local dev only.

## Developer Workflows
- **Local Dev:**
  - Run ETL/scraper jobs to update DB and/or `public/data/vehicles.json`.
  - Serve static frontend from `public/` for UI testing.
- **Prod Data:**
  - Frontend expects API or DB-backed source; do not deploy `public/data/vehicles.json` to S3.
- **DB Migrations:**
  - Use `database/migrate.py` and SQL files in `database/sql/migrations/`.
- **Lambda Scheduling:**
  - Use EventBridge Scheduler for daily refresh (see scheduling notes in codebase).

## Integration Points
- **External:**
  - AWS RDS (Postgres), S3, CloudFront, Lambda, EventBridge Scheduler.
  - Optional: AWS Secrets Manager for DB credentials.
- **Cross-Component:**
  - Filters and code mappings must stay in sync between backend (scraper, jobs) and frontend (JS, filter UI).
  - When adding new features, update `scraper/filters.py` and ensure parser logic in `scraper/code_parser.py` and frontend mapping in `public/app.js` are updated.

## Examples
- **Feature Extraction:** See `scraper/code_parser.py` for how image URLs are parsed to extract codes, then mapped to human labels using reverse maps built from `scraper/filters.py`.
- **Frontend Filtering:** See `public/app.js` for how selected filters are mapped to codes and used to filter the vehicle list.
- **DB Access:** See `database/db.py` for context-managed connection and logging pattern.

## Project-Specific Advice
- Always update filter code mappings in `scraper/filters.py` when adding new vehicle features.
- Prefer code-based feature extraction over deep scan for performance and cost.
- When refactoring, keep code-to-label mapping logic centralized for maintainability.
- Exclude `public/data/` from S3 deploys; use API for prod data.

---

If any section is unclear or missing, please provide feedback or specify which workflows, patterns, or integration points need more detail.