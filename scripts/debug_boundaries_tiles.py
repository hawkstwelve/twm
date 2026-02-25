#!/usr/bin/env python3
import argparse
import math
import re
import sqlite3
import subprocess
from dataclasses import dataclass


def xyz_to_bbox(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    n = 2 ** z
    lon_west = x / n * 360.0 - 180.0
    lon_east = (x + 1) / n * 360.0 - 180.0
    lat_north = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_south = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return (lon_west, lat_south, lon_east, lat_north)


def xyz_to_tms_y(z: int, y: int) -> int:
    return (2 ** z - 1) - y


def lonlat_to_xyz(z: int, lon: float, lat: float) -> tuple[int, int]:
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(max(min(lat, 85.05112878), -85.05112878))
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    x = max(0, min(n - 1, x))
    y = max(0, min(n - 1, y))
    return x, y


def parse_tile(value: str) -> tuple[int, int, int]:
    parts = value.split("/")
    if len(parts) != 3:
        raise ValueError(f"Invalid tile '{value}'. Expected z/x/y")
    z, x, y = (int(parts[0]), int(parts[1]), int(parts[2]))
    if z < 0 or x < 0 or y < 0:
        raise ValueError(f"Invalid tile '{value}'. z/x/y must be non-negative")
    return z, x, y


def parse_zooms(value: str) -> list[int]:
    zooms: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        zooms.append(int(part))
    if not zooms:
        raise ValueError("--zooms must contain at least one zoom value")
    return zooms


def query_tile_size(conn: sqlite3.Connection, z: int, x: int, y_xyz: int) -> int | None:
    y_tms = xyz_to_tms_y(z, y_xyz)
    row = conn.execute(
        "SELECT LENGTH(tile_data) FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?",
        (z, x, y_tms),
    ).fetchone()
    if not row:
        return None
    return int(row[0])


def ogr_feature_count(path: str, bbox: tuple[float, float, float, float]) -> tuple[int | None, str | None]:
    cmd = [
        "ogrinfo",
        "-ro",
        "-so",
        "-al",
        "-spat",
        f"{bbox[0]}",
        f"{bbox[1]}",
        f"{bbox[2]}",
        f"{bbox[3]}",
        path,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        return None, proc.stderr.strip() or proc.stdout.strip()
    match = re.search(r"Feature Count:\s*(\d+)", proc.stdout)
    if not match:
        return None, "Feature Count not found in ogrinfo output"
    return int(match.group(1)), None


def decode_tile_kinds(mbtiles_path: str, z: int, x: int, y: int) -> tuple[dict[str, int] | None, str | None]:
    cmd = ["tippecanoe-decode", mbtiles_path, str(z), str(x), str(y)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        return None, proc.stderr.strip() or proc.stdout.strip()

    text = proc.stdout
    kinds = [
        "coastline",
        "country",
        "state",
        "county",
        "great_lake_polygon",
        "great_lake_shoreline",
    ]
    counts: dict[str, int] = {}
    for kind in kinds:
        pattern = re.compile(rf'"kind"\s*:\s*"{re.escape(kind)}"')
        count = len(pattern.findall(text))
        if count > 0:
            counts[kind] = count
    return counts, None


@dataclass
class TileDebugResult:
    tile: str
    bbox: tuple[float, float, float, float]
    mbtiles_size: int | None
    source_coastline_count: int | None
    source_country_count: int | None
    decoded_kinds: dict[str, int] | None
    notes: list[str]


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug boundaries tile presence vs source intersections")
    parser.add_argument("--mbtiles", required=True, help="Path to twf_boundaries.mbtiles")
    parser.add_argument("--coastline-source", required=True, help="Path to coastline_lines.geojson")
    parser.add_argument("--country-source", required=True, help="Path to country_lines.geojson")
    parser.add_argument("--point", nargs=3, action="append", metavar=("NAME", "LON", "LAT"), default=[], help="Named point for scan (e.g. --point pnw -124.0 47.0)")
    parser.add_argument("--zooms", default="5,6,7", help="Comma-separated zooms used with --point scans")
    parser.add_argument("--scan-radius", type=int, default=1, help="Neighbor radius around each point tile (1 => 3x3)")
    parser.add_argument("tiles", nargs="*", help="Tile(s) in z/x/y format")
    args = parser.parse_args()

    if not args.tiles and not args.point:
        raise SystemExit("Provide at least one tile or one --point")

    zooms = parse_zooms(args.zooms)
    requested_tiles: list[str] = list(args.tiles)

    for point_name, lon_raw, lat_raw in args.point:
        lon = float(lon_raw)
        lat = float(lat_raw)
        for z in zooms:
            base_x, base_y = lonlat_to_xyz(z, lon, lat)
            n = 2 ** z
            for dx in range(-args.scan_radius, args.scan_radius + 1):
                for dy in range(-args.scan_radius, args.scan_radius + 1):
                    x = base_x + dx
                    y = base_y + dy
                    if x < 0 or y < 0 or x >= n or y >= n:
                        continue
                    requested_tiles.append(f"{z}/{x}/{y}")

    seen: set[str] = set()
    deduped_tiles: list[str] = []
    for tile in requested_tiles:
        if tile in seen:
            continue
        seen.add(tile)
        deduped_tiles.append(tile)

    conn = sqlite3.connect(args.mbtiles)

    results: list[TileDebugResult] = []
    for tile in deduped_tiles:
        z, x, y = parse_tile(tile)
        bbox = xyz_to_bbox(z, x, y)
        notes: list[str] = []

        mbtiles_size = query_tile_size(conn, z, x, y)
        if mbtiles_size is None:
            notes.append("mbtiles-miss")
        else:
            notes.append("mbtiles-hit")

        coastline_count, coastline_err = ogr_feature_count(args.coastline_source, bbox)
        if coastline_err:
            notes.append(f"coastline-source-error:{coastline_err}")

        country_count, country_err = ogr_feature_count(args.country_source, bbox)
        if country_err:
            notes.append(f"country-source-error:{country_err}")

        decoded_kinds = None
        if mbtiles_size is not None:
            decoded_kinds, decode_err = decode_tile_kinds(args.mbtiles, z, x, y)
            if decode_err:
                notes.append(f"decode-error:{decode_err}")

        if mbtiles_size is None and ((coastline_count or 0) > 0 or (country_count or 0) > 0):
            notes.append("unexpected-miss-source-has-features")
        elif mbtiles_size is None:
            notes.append("expected-miss-no-source-features")

        results.append(
            TileDebugResult(
                tile=tile,
                bbox=bbox,
                mbtiles_size=mbtiles_size,
                source_coastline_count=coastline_count,
                source_country_count=country_count,
                decoded_kinds=decoded_kinds,
                notes=notes,
            )
        )

    conn.close()

    for result in results:
        print(f"== {result.tile} ==")
        print(
            "bbox="
            f"{result.bbox[0]:.6f},{result.bbox[1]:.6f},{result.bbox[2]:.6f},{result.bbox[3]:.6f}"
        )
        print(f"mbtiles_size={result.mbtiles_size}")
        print(f"source_coastline_features={result.source_coastline_count}")
        print(f"source_country_features={result.source_country_count}")
        print(f"decoded_kinds={result.decoded_kinds}")
        print(f"notes={';'.join(result.notes)}")
        print()

    unexpected = [r for r in results if "unexpected-miss-source-has-features" in r.notes]
    print(f"summary total_tiles={len(results)} unexpected_misses={len(unexpected)}")
    if unexpected:
        print("summary unexpected_tiles=" + ",".join(r.tile for r in unexpected))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())