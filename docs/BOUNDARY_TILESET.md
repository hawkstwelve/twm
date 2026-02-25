# Boundary Tileset (Topology-First)

This repo now supports a canonical vector tileset for border linework and Great Lakes hydro geometry.

## Runtime endpoints

Served by `backend/app/services/tile_server.py`:

- `GET /tiles/v3/boundaries/v1/tilejson.json`
- `GET /tiles/v3/boundaries/v1/{z}/{x}/{y}.mvt`

## Frontend source model

`frontend/models-v3/src/components/map-canvas.tsx` now uses one vector source:

- `https://api.theweathermodels.com/tiles/v3/boundaries/v1/tilejson.json`

Expected source layers:

- `boundaries` with `kind` in `country|state|county`
- `hydro` with `kind` in `coastline|great_lake_polygon|great_lake_shoreline`

## Build script

Build with:

```bash
./scripts/build_boundaries_tileset.sh
```

Output MBTiles path:

- `data/v3/boundaries/v1/twf_boundaries.mbtiles`

## Zoom strategy (hard minzoom/maxzoom)

Implemented in the build script via separate tippecanoe passes and `tile-join`:

- Country boundaries: `z0-z10`
- Coastline: `z0-z10` (continuity-preserving build: no feature/tile size pruning, seam-safe `--no-clipping`, large buffer)
- State boundaries: `z3-z8`
- County boundaries low detail: `z5-z7`
- County boundaries high detail: `z8-z10`
- Great Lakes polygons: `z3-z8`
- Great Lakes shoreline: `z3-z10`

## Simplification and artifact controls

The build pipeline applies:

- Topology cleanup and snapping with `mapshaper` before tiling
- Strong county simplification at low zoom (`county_lines_low.geojson`)
- Additional county simplification tier for higher zoom (`county_lines_high.geojson`)
- Tippecanoe artifact/payload controls:
  - `--drop-smallest-as-needed`
  - `--coalesce-smallest-as-needed`
  - `--coalesce-densest-as-needed`
  - `--buffer` tuned small (`5-6`)

## Deployment

Set tile server env vars in:

- `deployment/systemd/tile-server.env.example`

Important for browsers: set `TWF_V3_TILES_PUBLIC_BASE_URL` so TileJSON emits absolute tile URLs (not relative `/tiles/...`).

Then restart tile server unit:

```bash
sudo systemctl daemon-reload
sudo systemctl restart twf-v3-tile-server
```

## Notes

- CARTO vector boundaries and runtime Plotly counties GeoJSON are no longer used by the frontend boundary stack.
- CARTO raster basemap/labels remain in use.
- If a source schema changes, keep the `kind` taxonomy stable so frontend filters continue to work.
