from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CityCandidate:
    name: str
    lat: float
    lon: float
    population: int


@dataclass(frozen=True)
class SelectedAnchor:
    state_code: str
    state_name: str
    city_name: str
    lat: float
    lon: float
    index: int
    selection_mode: str


def city(name: str, lat: float, lon: float, population: int) -> CityCandidate:
    return CityCandidate(name=name, lat=lat, lon=lon, population=population)


CONUS_STATE_ORDER: tuple[str, ...] = (
    "AL",
    "AR",
    "AZ",
    "CA",
    "CO",
    "CT",
    "DC",
    "DE",
    "FL",
    "GA",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MS",
    "MT",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
)

STATE_NAMES: dict[str, str] = {
    "AL": "Alabama",
    "AR": "Arkansas",
    "AZ": "Arizona",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "District of Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "IA": "Iowa",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MO": "Missouri",
    "MS": "Mississippi",
    "MT": "Montana",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "NE": "Nebraska",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NV": "Nevada",
    "NY": "New York",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VA": "Virginia",
    "VT": "Vermont",
    "WA": "Washington",
    "WI": "Wisconsin",
    "WV": "West Virginia",
    "WY": "Wyoming",
}

STATE_CITY_CANDIDATES: dict[str, list[CityCandidate]] = {
    "AL": [
        city("Birmingham", 33.5186, -86.8104, 1_120_000),
        city("Huntsville", 34.7304, -86.5861, 530_000),
        city("Mobile", 30.6954, -88.0399, 430_000),
        city("Montgomery", 32.3792, -86.3077, 390_000),
    ],
    "AR": [
        city("Little Rock", 34.7465, -92.2896, 750_000),
        city("Fayetteville", 36.0822, -94.1719, 580_000),
        city("Fort Smith", 35.3859, -94.3985, 300_000),
        city("Jonesboro", 35.8423, -90.7043, 135_000),
    ],
    "AZ": [
        city("Phoenix", 33.4484, -112.0740, 4_950_000),
        city("Tucson", 32.2226, -110.9747, 1_040_000),
        city("Flagstaff", 35.1983, -111.6513, 145_000),
        city("Yuma", 32.6927, -114.6277, 200_000),
    ],
    "CA": [
        city("Los Angeles", 34.0522, -118.2437, 13_200_000),
        city("San Francisco", 37.7749, -122.4194, 4_750_000),
        city("San Diego", 32.7157, -117.1611, 3_300_000),
        city("Sacramento", 38.5816, -121.4944, 2_400_000),
        city("Fresno", 36.7378, -119.7871, 1_000_000),
    ],
    "CO": [
        city("Denver", 39.7392, -104.9903, 2_980_000),
        city("Colorado Springs", 38.8339, -104.8214, 760_000),
        city("Fort Collins", 40.5853, -105.0844, 370_000),
        city("Grand Junction", 39.0639, -108.5506, 160_000),
    ],
    "CT": [
        city("Hartford", 41.7658, -72.6734, 1_200_000),
        city("New Haven", 41.3083, -72.9279, 860_000),
        city("Bridgeport", 41.1792, -73.1894, 950_000),
    ],
    "DC": [
        city("Washington", 38.9072, -77.0369, 6_300_000),
    ],
    "DE": [
        city("Wilmington", 39.7391, -75.5398, 720_000),
        city("Dover", 39.1582, -75.5244, 180_000),
    ],
    "FL": [
        city("Miami", 25.7617, -80.1918, 6_140_000),
        city("Tampa", 27.9506, -82.4572, 3_260_000),
        city("Orlando", 28.5383, -81.3792, 2_940_000),
        city("Jacksonville", 30.3322, -81.6557, 1_680_000),
        city("Tallahassee", 30.4383, -84.2807, 390_000),
    ],
    "GA": [
        city("Atlanta", 33.7490, -84.3880, 6_300_000),
        city("Augusta", 33.4735, -82.0105, 630_000),
        city("Savannah", 32.0809, -81.0912, 420_000),
        city("Columbus", 32.4609, -84.9877, 330_000),
    ],
    "IA": [
        city("Des Moines", 41.5868, -93.6250, 720_000),
        city("Davenport", 41.5236, -90.5776, 380_000),
        city("Cedar Rapids", 41.9779, -91.6656, 280_000),
        city("Sioux City", 42.4990, -96.4003, 145_000),
    ],
    "ID": [
        city("Boise", 43.6150, -116.2023, 830_000),
        city("Coeur d'Alene", 47.6777, -116.7805, 170_000),
        city("Idaho Falls", 43.4917, -112.0333, 155_000),
        city("Pocatello", 42.8713, -112.4455, 90_000),
    ],
    "IL": [
        city("Chicago", 41.8781, -87.6298, 9_600_000),
        city("Peoria", 40.6936, -89.5890, 400_000),
        city("Rockford", 42.2711, -89.0937, 340_000),
        city("Springfield", 39.7817, -89.6501, 210_000),
    ],
    "IN": [
        city("Indianapolis", 39.7684, -86.1581, 2_100_000),
        city("Fort Wayne", 41.0793, -85.1394, 430_000),
        city("South Bend", 41.6764, -86.2520, 320_000),
        city("Evansville", 37.9716, -87.5711, 310_000),
    ],
    "KS": [
        city("Wichita", 37.6872, -97.3301, 650_000),
        city("Kansas City", 39.1142, -94.6275, 1_700_000),
        city("Topeka", 39.0473, -95.6752, 230_000),
        city("Manhattan", 39.1836, -96.5717, 100_000),
    ],
    "KY": [
        city("Louisville", 38.2527, -85.7585, 1_370_000),
        city("Lexington", 38.0406, -84.5037, 530_000),
        city("Bowling Green", 36.9685, -86.4808, 180_000),
    ],
    "LA": [
        city("New Orleans", 29.9511, -90.0715, 1_270_000),
        city("Baton Rouge", 30.4515, -91.1871, 870_000),
        city("Lafayette", 30.2241, -92.0198, 490_000),
        city("Shreveport", 32.5252, -93.7502, 390_000),
    ],
    "MA": [
        city("Boston", 42.3601, -71.0589, 4_940_000),
        city("Worcester", 42.2626, -71.8023, 940_000),
        city("Springfield", 42.1015, -72.5898, 690_000),
    ],
    "MD": [
        city("Baltimore", 39.2904, -76.6122, 2_840_000),
        city("Hagerstown", 39.6418, -77.7199, 270_000),
        city("Salisbury", 38.3607, -75.5994, 110_000),
    ],
    "ME": [
        city("Portland", 43.6591, -70.2568, 550_000),
        city("Bangor", 44.8012, -68.7778, 150_000),
        city("Augusta", 44.3106, -69.7795, 120_000),
    ],
    "MI": [
        city("Detroit", 42.3314, -83.0458, 4_300_000),
        city("Grand Rapids", 42.9634, -85.6681, 1_100_000),
        city("Lansing", 42.7325, -84.5555, 540_000),
        city("Traverse City", 44.7631, -85.6206, 150_000),
    ],
    "MN": [
        city("Minneapolis", 44.9778, -93.2650, 3_700_000),
        city("Duluth", 46.7867, -92.1005, 280_000),
        city("Rochester", 44.0121, -92.4802, 230_000),
        city("St. Cloud", 45.5579, -94.1632, 200_000),
    ],
    "MO": [
        city("St. Louis", 38.6270, -90.1994, 2_800_000),
        city("Kansas City", 39.0997, -94.5786, 2_200_000),
        city("Springfield", 37.2089, -93.2923, 500_000),
        city("Columbia", 38.9517, -92.3341, 180_000),
    ],
    "MS": [
        city("Jackson", 32.2988, -90.1848, 590_000),
        city("Gulfport", 30.3674, -89.0928, 420_000),
        city("Hattiesburg", 31.3271, -89.2903, 170_000),
        city("Tupelo", 34.2576, -88.7034, 140_000),
    ],
    "MT": [
        city("Billings", 45.7833, -108.5007, 180_000),
        city("Bozeman", 45.6770, -111.0429, 130_000),
        city("Missoula", 46.8721, -113.9940, 120_000),
        city("Great Falls", 47.5053, -111.3008, 90_000),
    ],
    "NC": [
        city("Charlotte", 35.2271, -80.8431, 2_800_000),
        city("Raleigh", 35.7796, -78.6382, 1_500_000),
        city("Greensboro", 36.0726, -79.7920, 780_000),
        city("Asheville", 35.5951, -82.5515, 470_000),
        city("Wilmington", 34.2257, -77.9447, 300_000),
    ],
    "ND": [
        city("Fargo", 46.8772, -96.7898, 260_000),
        city("Bismarck", 46.8083, -100.7837, 130_000),
        city("Grand Forks", 47.9253, -97.0329, 100_000),
    ],
    "NE": [
        city("Omaha", 41.2565, -95.9345, 990_000),
        city("Lincoln", 40.8136, -96.7026, 340_000),
        city("Grand Island", 40.9264, -98.3420, 80_000),
    ],
    "NH": [
        city("Manchester", 42.9956, -71.4548, 420_000),
        city("Concord", 43.2081, -71.5376, 150_000),
        city("Portsmouth", 43.0718, -70.7626, 130_000),
    ],
    "NJ": [
        city("Newark", 40.7357, -74.1724, 2_100_000),
        city("Camden", 39.9259, -75.1196, 520_000),
        city("Trenton", 40.2171, -74.7429, 370_000),
        city("Atlantic City", 39.3643, -74.4229, 270_000),
    ],
    "NM": [
        city("Albuquerque", 35.0844, -106.6504, 920_000),
        city("Las Cruces", 32.3199, -106.7637, 220_000),
        city("Santa Fe", 35.6870, -105.9378, 150_000),
    ],
    "NV": [
        city("Las Vegas", 36.1699, -115.1398, 2_330_000),
        city("Reno", 39.5296, -119.8138, 530_000),
        city("Elko", 40.8324, -115.7631, 55_000),
    ],
    "NY": [
        city("New York", 40.7128, -74.0060, 19_600_000),
        city("Buffalo", 42.8864, -78.8784, 1_100_000),
        city("Rochester", 43.1566, -77.6088, 1_000_000),
        city("Albany", 42.6526, -73.7562, 880_000),
        city("Syracuse", 43.0481, -76.1474, 660_000),
    ],
    "OH": [
        city("Columbus", 39.9612, -82.9988, 2_100_000),
        city("Cleveland", 41.4993, -81.6944, 2_000_000),
        city("Cincinnati", 39.1031, -84.5120, 1_800_000),
        city("Toledo", 41.6528, -83.5379, 610_000),
    ],
    "OK": [
        city("Oklahoma City", 35.4676, -97.5164, 1_450_000),
        city("Tulsa", 36.1540, -95.9928, 1_020_000),
        city("Lawton", 34.6036, -98.3959, 130_000),
    ],
    "OR": [
        city("Portland", 45.5152, -122.6784, 2_510_000),
        city("Eugene", 44.0521, -123.0868, 380_000),
        city("Medford", 42.3265, -122.8756, 220_000),
        city("Bend", 44.0582, -121.3153, 200_000),
    ],
    "PA": [
        city("Philadelphia", 39.9526, -75.1652, 6_240_000),
        city("Pittsburgh", 40.4406, -79.9959, 2_370_000),
        city("Harrisburg", 40.2732, -76.8867, 590_000),
        city("Scranton", 41.4089, -75.6624, 570_000),
        city("Erie", 42.1292, -80.0851, 270_000),
    ],
    "RI": [
        city("Providence", 41.8240, -71.4128, 1_680_000),
    ],
    "SC": [
        city("Charleston", 32.7765, -79.9311, 840_000),
        city("Columbia", 34.0007, -81.0348, 860_000),
        city("Greenville", 34.8526, -82.3940, 930_000),
        city("Myrtle Beach", 33.6891, -78.8867, 500_000),
    ],
    "SD": [
        city("Sioux Falls", 43.5446, -96.7311, 290_000),
        city("Rapid City", 44.0805, -103.2310, 150_000),
        city("Aberdeen", 45.4647, -98.4865, 40_000),
    ],
    "TN": [
        city("Nashville", 36.1627, -86.7816, 2_100_000),
        city("Memphis", 35.1495, -90.0490, 1_350_000),
        city("Knoxville", 35.9606, -83.9207, 880_000),
        city("Chattanooga", 35.0456, -85.3097, 570_000),
    ],
    "TX": [
        city("Dallas", 32.7767, -96.7970, 7_800_000),
        city("Houston", 29.7604, -95.3698, 7_100_000),
        city("San Antonio", 29.4241, -98.4936, 2_600_000),
        city("Austin", 30.2672, -97.7431, 2_400_000),
        city("El Paso", 31.7619, -106.4850, 870_000),
    ],
    "UT": [
        city("Salt Lake City", 40.7608, -111.8910, 1_250_000),
        city("Provo", 40.2338, -111.6585, 670_000),
        city("St. George", 37.0965, -113.5684, 200_000),
    ],
    "VA": [
        city("Richmond", 37.5407, -77.4360, 1_300_000),
        city("Virginia Beach", 36.8529, -75.9780, 1_800_000),
        city("Roanoke", 37.2709, -79.9414, 320_000),
        city("Charlottesville", 38.0293, -78.4767, 230_000),
    ],
    "VT": [
        city("Burlington", 44.4759, -73.2121, 220_000),
        city("Rutland", 43.6106, -72.9726, 60_000),
    ],
    "WA": [
        city("Seattle", 47.6062, -122.3321, 4_050_000),
        city("Spokane", 47.6588, -117.4260, 590_000),
        city("Tri-Cities", 46.2300, -119.0900, 300_000),
        city("Bellingham", 48.7519, -122.4787, 220_000),
    ],
    "WI": [
        city("Milwaukee", 43.0389, -87.9065, 1_560_000),
        city("Madison", 43.0731, -89.4012, 690_000),
        city("Green Bay", 44.5133, -88.0133, 320_000),
        city("Eau Claire", 44.8113, -91.4985, 170_000),
    ],
    "WV": [
        city("Charleston", 38.3498, -81.6326, 250_000),
        city("Huntington", 38.4192, -82.4452, 360_000),
        city("Morgantown", 39.6295, -79.9559, 140_000),
    ],
    "WY": [
        city("Cheyenne", 41.1400, -104.8202, 100_000),
        city("Jackson", 43.4799, -110.7624, 110_000),
        city("Casper", 42.8501, -106.3252, 80_000),
    ],
}

FORCE_TWO_ANCHOR_STATES: set[str] = {
    "AZ",
    "CA",
    "FL",
    "GA",
    "MI",
    "MN",
    "MO",
    "NC",
    "NV",
    "NY",
    "OH",
    "PA",
    "TN",
    "TX",
    "VA",
    "WA",
    "WI",
}

PRIMARY_CITY_OVERRIDES: dict[str, str] = {
    "CO": "Denver",
    "GA": "Atlanta",
    "IL": "Chicago",
    "KS": "Wichita",
    "OR": "Portland",
    "SC": "Charleston",
    "VA": "Richmond",
    "WA": "Seattle",
    "WV": "Charleston",
    "WY": "Cheyenne",
}

SECONDARY_CITY_OVERRIDES: dict[str, str] = {
    "FL": "Tampa",
    "GA": "Savannah",
    "NC": "Raleigh",
    "NY": "Buffalo",
    "OH": "Cleveland",
    "PA": "Pittsburgh",
    "TX": "Houston",
    "VA": "Virginia Beach",
    "WA": "Spokane",
    "WI": "Madison",
}


def haversine_km(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    radius_km = 6371.0
    phi_a = math.radians(lat_a)
    phi_b = math.radians(lat_b)
    delta_phi = math.radians(lat_b - lat_a)
    delta_lambda = math.radians(lon_b - lon_a)
    sin_phi = math.sin(delta_phi / 2.0)
    sin_lambda = math.sin(delta_lambda / 2.0)
    arc = sin_phi * sin_phi + math.cos(phi_a) * math.cos(phi_b) * sin_lambda * sin_lambda
    return 2.0 * radius_km * math.asin(math.sqrt(arc))


def candidate_by_name(state_code: str, city_name: str) -> CityCandidate:
    for candidate in STATE_CITY_CANDIDATES[state_code]:
        if candidate.name == city_name:
            return candidate
    raise KeyError(f"Unknown candidate city {city_name!r} for state {state_code}")


def select_primary(state_code: str, candidates: list[CityCandidate]) -> tuple[CityCandidate, str]:
    override_name = PRIMARY_CITY_OVERRIDES.get(state_code)
    if override_name:
        return candidate_by_name(state_code, override_name), "manual-primary"
    return max(candidates, key=lambda candidate: candidate.population), "population-primary"


def second_anchor_score(primary: CityCandidate, candidate: CityCandidate, top_population: int) -> float:
    distance_km = haversine_km(primary.lat, primary.lon, candidate.lat, candidate.lon)
    population_score = candidate.population / top_population
    distance_score = min(distance_km, 800.0) / 800.0
    proximity_penalty = 0.30 if distance_km < 140.0 else 0.0
    return population_score * 0.72 + distance_score * 0.28 - proximity_penalty


def select_secondary(
    state_code: str,
    primary: CityCandidate,
    candidates: list[CityCandidate],
) -> tuple[CityCandidate, str]:
    override_name = SECONDARY_CITY_OVERRIDES.get(state_code)
    if override_name:
        return candidate_by_name(state_code, override_name), "manual-secondary"

    remaining_candidates = [candidate for candidate in candidates if candidate.name != primary.name]
    top_population = max(candidate.population for candidate in candidates)
    selected = max(
        remaining_candidates,
        key=lambda candidate: second_anchor_score(primary, candidate, top_population),
    )
    return selected, "greedy-secondary"


def target_anchor_count(state_code: str, candidates: list[CityCandidate]) -> int:
    if state_code in FORCE_TWO_ANCHOR_STATES and len(candidates) >= 2:
        return 2
    return 1


def build_selected_anchors() -> list[SelectedAnchor]:
    selected_anchors: list[SelectedAnchor] = []

    for state_code in CONUS_STATE_ORDER:
        state_name = STATE_NAMES[state_code]
        candidates = STATE_CITY_CANDIDATES[state_code]
        primary, primary_mode = select_primary(state_code, candidates)
        selected_anchors.append(
            SelectedAnchor(
                state_code=state_code,
                state_name=state_name,
                city_name=primary.name,
                lat=primary.lat,
                lon=primary.lon,
                index=1,
                selection_mode=primary_mode,
            )
        )

        if target_anchor_count(state_code, candidates) != 2:
            continue

        secondary, secondary_mode = select_secondary(state_code, primary, candidates)
        selected_anchors.append(
            SelectedAnchor(
                state_code=state_code,
                state_name=state_name,
                city_name=secondary.name,
                lat=secondary.lat,
                lon=secondary.lon,
                index=2,
                selection_mode=secondary_mode,
            )
        )

    return selected_anchors


def build_geojson(selected_anchors: list[SelectedAnchor]) -> dict[str, object]:
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": f"{anchor.state_code}_{anchor.index}",
                "properties": {
                    "st": anchor.state_code,
                    "state": anchor.state_name,
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [anchor.lon, anchor.lat],
                },
            }
            for anchor in selected_anchors
        ],
    }


def print_summary(selected_anchors: list[SelectedAnchor]) -> None:
    per_state: dict[str, list[SelectedAnchor]] = {state_code: [] for state_code in CONUS_STATE_ORDER}
    for anchor in selected_anchors:
        per_state[anchor.state_code].append(anchor)

    total_anchors = len(selected_anchors)
    if total_anchors < 60 or total_anchors > 90:
        raise ValueError(f"Anchor count {total_anchors} is outside the expected sparse target range")

    print(f"total_anchors={total_anchors}")
    print("per_state_counts=")
    for state_code in CONUS_STATE_ORDER:
        anchors = per_state[state_code]
        city_list = ", ".join(anchor.city_name for anchor in anchors)
        print(f"  {state_code}: {len(anchors)} [{city_list}]")

    applied_overrides = [
        anchor for anchor in selected_anchors if anchor.selection_mode.startswith("manual-")
    ]
    print("manual_overrides=")
    for anchor in applied_overrides:
        print(f"  {anchor.state_code}_{anchor.index}: {anchor.city_name} ({anchor.selection_mode})")


def default_output_path() -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "frontend" / "public" / "data" / "anchors_conus.geojson"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a sparse metro-weighted CONUS anchor catalog")
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output_path(),
        help="Path to the anchors_conus.geojson output file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    selected_anchors = build_selected_anchors()
    geojson = build_geojson(selected_anchors)
    args.output.write_text(f"{json.dumps(geojson, indent=2)}\n", encoding="utf-8")
    print_summary(selected_anchors)


if __name__ == "__main__":
    main()