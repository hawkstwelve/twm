#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="${ROOT_DIR}/data/v3/boundaries/work"
SOURCE_DIR="${WORK_DIR}/source"
BUILD_DIR="${WORK_DIR}/build"
TMP_DIR="${WORK_DIR}/tmp"
OUT_DIR="${ROOT_DIR}/data/v3/boundaries/v1"
OUT_MBTILES="${OUT_DIR}/twf_boundaries.mbtiles"

for cmd in curl unzip ogr2ogr mapshaper tippecanoe tile-join sqlite3; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
done

mkdir -p "$SOURCE_DIR" "$BUILD_DIR" "$TMP_DIR" "$OUT_DIR"
rm -f "$TMP_DIR"/*.mbtiles

COUNTRY_URL="https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_0_boundary_lines_land.geojson"
COAST_URL="https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_coastline.geojson"
LAKES_URL="https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_lakes.geojson"
STATES_ZIP_URL="https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_state_5m.zip"
COUNTIES_ZIP_URL="https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_county_5m.zip"

curl -L "$COUNTRY_URL" -o "$SOURCE_DIR/country_lines.geojson"
curl -L "$COAST_URL" -o "$SOURCE_DIR/coastline.geojson"
curl -L "$LAKES_URL" -o "$SOURCE_DIR/lakes.geojson"
curl -L "$STATES_ZIP_URL" -o "$SOURCE_DIR/states.zip"
curl -L "$COUNTIES_ZIP_URL" -o "$SOURCE_DIR/counties.zip"

unzip -o "$SOURCE_DIR/states.zip" -d "$SOURCE_DIR/states_shp" >/dev/null
unzip -o "$SOURCE_DIR/counties.zip" -d "$SOURCE_DIR/counties_shp" >/dev/null

ogr2ogr -f GeoJSON "$BUILD_DIR/states_polygons.geojson" "$SOURCE_DIR/states_shp/cb_2023_us_state_5m.shp" -t_srs EPSG:4326 -select GEOID,NAME,STUSPS
ogr2ogr -f GeoJSON "$BUILD_DIR/counties_polygons.geojson" "$SOURCE_DIR/counties_shp/cb_2023_us_county_5m.shp" -t_srs EPSG:4326 -select GEOID,NAME,STATEFP,COUNTYFP

mapshaper "$SOURCE_DIR/country_lines.geojson" -snap interval=0.00005 -clean -each 'kind="country";admin_level=2' -filter-fields kind,admin_level -o format=geojson "$BUILD_DIR/country_lines.geojson"
mapshaper "$SOURCE_DIR/coastline.geojson" -snap interval=0.00005 -clean -each 'kind="coastline"' -filter-fields kind -o format=geojson "$BUILD_DIR/coastline_lines.geojson"

mapshaper "$BUILD_DIR/states_polygons.geojson" -snap interval=0.00005 -clean -innerlines -each 'kind="state";admin_level=4' -filter-fields kind,admin_level -o format=geojson "$BUILD_DIR/state_lines.geojson"
mapshaper "$BUILD_DIR/counties_polygons.geojson" -snap interval=0.00003 -clean -innerlines -each 'kind="county";admin_level=6' -filter-fields kind,admin_level -o format=geojson "$BUILD_DIR/county_lines_raw.geojson"

mapshaper "$BUILD_DIR/county_lines_raw.geojson" -snap interval=0.00003 -clean -simplify weighted 8% keep-shapes -o format=geojson "$BUILD_DIR/county_lines_low.geojson"
mapshaper "$BUILD_DIR/county_lines_raw.geojson" -snap interval=0.00003 -clean -simplify weighted 22% keep-shapes -o format=geojson "$BUILD_DIR/county_lines_high.geojson"

mapshaper "$SOURCE_DIR/lakes.geojson" -snap interval=0.00005 -clean -filter 'name=="Lake Superior" || name=="Lake Michigan" || name=="Lake Huron" || name=="Lake Erie" || name=="Lake Ontario" || name_en=="Lake Superior" || name_en=="Lake Michigan" || name_en=="Lake Huron" || name_en=="Lake Erie" || name_en=="Lake Ontario"' -each 'kind="great_lake_polygon"' -filter-fields kind,name,name_en -o format=geojson "$BUILD_DIR/great_lake_polygons.geojson"
mapshaper "$BUILD_DIR/great_lake_polygons.geojson" -snap interval=0.00005 -clean -lines -each 'kind="great_lake_shoreline"' -filter-fields kind -o format=geojson "$BUILD_DIR/great_lake_shoreline.geojson"

# Keep country/coastline buffers modest to reduce MapLibre "Geometry exceeds allowed extent" decode warnings.
tippecanoe -f -o "$TMP_DIR/boundary_country.mbtiles" -l boundaries -Z0 -z10 --buffer=4 --detect-shared-borders --no-feature-limit --no-tile-size-limit "$BUILD_DIR/country_lines.geojson"
tippecanoe -f -o "$TMP_DIR/boundary_state.mbtiles" -l boundaries -Z3 -z8 --buffer=6 --detect-shared-borders --drop-smallest-as-needed --coalesce-densest-as-needed "$BUILD_DIR/state_lines.geojson"
tippecanoe -f -o "$TMP_DIR/boundary_county_low.mbtiles" -l counties -Z5 -z7 --buffer=4 --drop-smallest-as-needed --coalesce-smallest-as-needed --coalesce-densest-as-needed --simplification=10 "$BUILD_DIR/county_lines_low.geojson"
tippecanoe -f -o "$TMP_DIR/boundary_county_high.mbtiles" -l counties -Z8 -z10 --buffer=4 --drop-smallest-as-needed --coalesce-smallest-as-needed --coalesce-densest-as-needed --simplification=6 "$BUILD_DIR/county_lines_high.geojson"

tippecanoe -f -o "$TMP_DIR/hydro_polygon.mbtiles" -l hydro -Z3 -z8 --buffer=6 --drop-smallest-as-needed --coalesce-densest-as-needed "$BUILD_DIR/great_lake_polygons.geojson"
tippecanoe -f -o "$TMP_DIR/hydro_shoreline.mbtiles" -l hydro -Z3 -z10 --buffer=6 --drop-smallest-as-needed --coalesce-densest-as-needed "$BUILD_DIR/great_lake_shoreline.geojson"
tippecanoe -f -o "$TMP_DIR/hydro_coastline.mbtiles" -l hydro -Z0 -z10 --buffer=4 --no-feature-limit --no-tile-size-limit "$BUILD_DIR/coastline_lines.geojson"

tile-join -f -o "$OUT_MBTILES" \
  "$TMP_DIR/boundary_country.mbtiles" \
  "$TMP_DIR/boundary_state.mbtiles" \
  "$TMP_DIR/boundary_county_low.mbtiles" \
  "$TMP_DIR/boundary_county_high.mbtiles" \
  "$TMP_DIR/hydro_polygon.mbtiles" \
  "$TMP_DIR/hydro_shoreline.mbtiles" \
  "$TMP_DIR/hydro_coastline.mbtiles"

VECTOR_LAYERS='[{"id":"boundaries","description":"country/state linework","fields":{"kind":"String","admin_level":"Number"},"minzoom":0,"maxzoom":10},{"id":"counties","description":"county linework","fields":{"kind":"String","admin_level":"Number"},"minzoom":5,"maxzoom":10},{"id":"hydro","description":"coastline and Great Lakes polygon/shoreline","fields":{"kind":"String"},"minzoom":0,"maxzoom":10}]'
sqlite3 "$OUT_MBTILES" "INSERT OR REPLACE INTO metadata(name,value) VALUES('name','TWF Boundaries v1');"
sqlite3 "$OUT_MBTILES" "INSERT OR REPLACE INTO metadata(name,value) VALUES('id','twf-boundaries-v1');"
sqlite3 "$OUT_MBTILES" "INSERT OR REPLACE INTO metadata(name,value) VALUES('description','Canonical boundary + hydro tileset for TWF V3');"
sqlite3 "$OUT_MBTILES" "INSERT OR REPLACE INTO metadata(name,value) VALUES('attribution','Natural Earth; U.S. Census Bureau TIGER/Cartographic Boundary');"
sqlite3 "$OUT_MBTILES" "INSERT OR REPLACE INTO metadata(name,value) VALUES('minzoom','0');"
sqlite3 "$OUT_MBTILES" "INSERT OR REPLACE INTO metadata(name,value) VALUES('maxzoom','10');"
sqlite3 "$OUT_MBTILES" "INSERT OR REPLACE INTO metadata(name,value) VALUES('bounds','-180,-85.0511,180,85.0511');"
sqlite3 "$OUT_MBTILES" "INSERT OR REPLACE INTO metadata(name,value) VALUES('center','-98.58,39.83,4');"
sqlite3 "$OUT_MBTILES" "INSERT OR REPLACE INTO metadata(name,value) VALUES('format','pbf');"
sqlite3 "$OUT_MBTILES" "INSERT OR REPLACE INTO metadata(name,value) VALUES('vector_layers','$VECTOR_LAYERS');"

echo "Built boundaries tileset: $OUT_MBTILES"
