# IDS-DRR Paraguay (Idris) — backend deployment data

Demo deployment data for the IDS-DRR platform configured for Paraguay
("Idris"). No custom Django plugin code — this package is data-only.

## Layout

```
ids-drr-paraguay-data/
  config.toml                       Paths + state config
  geography/
    departments.geojson             18 departments (DISTRICT level)
    districts.geojson               263 districts (SUB DISTRICT level)
  indicators/
    Paraguay_indicators.csv         18 indicators across the Sendai pillars
  data/
    Paraguay_data.csv               Random demo values (3,156 rows = 263 × 12)
```

## Geography hierarchy

- **STATE** — `Paraguay` (single, stub created at import time)
- **DISTRICT** — 18 departments
- **SUB DISTRICT** — 263 districts

## Wiring it up with Docker Compose

In the repo root (or via your `.env`):

```bash
PLUGIN_PACKAGE=data-management/plugin-stub        # default; no custom views
BACKEND_CONFIG_DIR=./platform/ids-drr-paraguay-data
```

Then:

```bash
docker compose up -d backend
docker compose exec backend python manage.py import_geojson
docker compose exec backend python manage.py import_indicators
docker compose exec backend python manage.py import_data
```

## Notes

- The wildfire-hazard indicator slug is `flood-hazard` for symmetry with the existing frontend code paths; the displayed name is overridden in the branding's `factors.hazard.name` message key.
- Indicator values in `Paraguay_data.csv` are deterministically random (seed=42). Re-run the generator to change them; the script is in commit history.
- 12 monthly time periods, ending at `2026_03` (matches `default_time_period` in `config.toml`).
