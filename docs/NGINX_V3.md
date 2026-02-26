# Nginx routing (V3)

## Domains
- `api.theweathermodels.com`: V3 edge
- `legacy-api.theweathermodels.com`: legacy stack (kept separate)

## V3 routes on api.theweathermodels.com
- `/models-v3/` → static frontend build output
  - filesystem: `/opt/twf_v3/frontend/models-v3/dist/`
- `/api/v3/` → V3 API upstream
  - upstream: `http://127.0.0.1:8200`
- `/api/v4/` → V4 API upstream
  - upstream: `http://127.0.0.1:8200`
- `/tiles/v3/` → V3 tile server upstream
  - upstream: `http://127.0.0.1:8201`
  - includes vector boundary endpoints:
    - `/tiles/v3/boundaries/v1/tilejson.json`
    - `/tiles/v3/boundaries/v1/{z}/{x}/{y}.mvt`

## Retired V2 routes
V2 paths return `410 Gone`:
- `/models-v2/`
- `/api/` (old discovery)
- `/manifests/`
- `/published/`
- `/tiles/`
- `/frames/`
- `/api/v2/`, `/tiles/v2/`, `/tiles-titiler/`

## Notes
- V2 `/data/*` is not exposed by nginx in V3 routing.
- systemd units must use absolute venv paths (no pyenv shims).
- Loop WebP runtime URLs are emitted as `/api/v4/{model}/{run}/{var}/{fh}/loop.webp?tier=...`.
- Legacy `/loop/v3/*` static alias blocks can be retained temporarily, then removed after traffic confirms no active usage.
