import csv
import os
import zipfile
from pathlib import Path
from typing import Iterable

import dlt
import requests


BASE_URL = "https://opendata.samtrafiken.se/gtfs"
OPERATOR = "sl"


def _read_env_key(key: str) -> str | None:
    
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return os.getenv(key)
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k == key:
            return v
    return os.getenv(key)


def _download_gtfs(zip_path: Path) -> Path:
    api_key = _read_env_key("API_KEY")
    if not api_key:
        raise RuntimeError("API_KEY not found in .env or environment")
    url = f"{BASE_URL}/{OPERATOR}/{OPERATOR}.zip?key={api_key}"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    zip_path.write_bytes(resp.content)
    return zip_path


def _load_csv(zf: zipfile.ZipFile, name: str) -> Iterable[dict]:
    if name not in zf.namelist():
        raise FileNotFoundError(f"{name} not found in GTFS zip")
    with zf.open(name) as f:
        reader = csv.DictReader((line.decode("utf-8") for line in f))
        for row in reader:
            yield row


def _get_metro_route_ids(zf: zipfile.ZipFile) -> set[str]:
    route_ids: set[str] = set()
    for row in _load_csv(zf, "routes.txt"):
        if row.get("route_type") == "401":
            route_ids.add(row["route_id"])
    return route_ids


def _get_metro_trip_ids(zf: zipfile.ZipFile, route_ids: set[str]) -> set[str]:
    trip_ids: set[str] = set()
    for row in _load_csv(zf, "trips.txt"):
        if row.get("route_id") in route_ids:
            trip_ids.add(row["trip_id"])
    return trip_ids


@dlt.resource(write_disposition="replace")
def metro_routes(zf: zipfile.ZipFile):
    for row in _load_csv(zf, "routes.txt"):
        if row.get("route_type") == "401":
            row["_operator"] = OPERATOR
            yield row


@dlt.resource(write_disposition="replace")
def metro_trips(zf: zipfile.ZipFile, route_ids: set[str]):
    for row in _load_csv(zf, "trips.txt"):
        if row.get("route_id") in route_ids:
            row["_operator"] = OPERATOR
            yield row


@dlt.resource(write_disposition="replace")
def metro_stop_times(zf: zipfile.ZipFile, trip_ids: set[str]):
    for row in _load_csv(zf, "stop_times.txt"):
        if row.get("trip_id") in trip_ids:
            row["_operator"] = OPERATOR
            yield row


@dlt.resource(write_disposition="replace")
def metro_stops(zf: zipfile.ZipFile, stop_ids: set[str]):
    for row in _load_csv(zf, "stops.txt"):
        if row.get("stop_id") in stop_ids:
            row["_operator"] = OPERATOR
            yield row


def run_pipeline():
    data_dir = Path(__file__).resolve().parents[1] / "data" / "gtfs_static"
    zip_path = data_dir / f"{OPERATOR}.zip"
    _download_gtfs(zip_path)

    with zipfile.ZipFile(zip_path, "r") as zf:
        route_ids = _get_metro_route_ids(zf)
        trip_ids = _get_metro_trip_ids(zf, route_ids)

        
        stop_ids: set[str] = set()
        for row in _load_csv(zf, "stop_times.txt"):
            if row.get("trip_id") in trip_ids:
                stop_ids.add(row["stop_id"])

        print(f"Metro routes: {len(route_ids)}")
        print(f"Metro trips: {len(trip_ids)}")
        print(f"Metro stops: {len(stop_ids)}")

        pipeline = dlt.pipeline(
            pipeline_name="sl_gtfs_static",
            destination="snowflake",
            dataset_name="static",  # TRAFIK_DATA.STATIC
        )

        load_info = pipeline.run(
            [
                metro_routes(zf).with_name("metro_routes"),
                metro_trips(zf, route_ids).with_name("metro_trips"),
                metro_stop_times(zf, trip_ids).with_name("metro_stop_times"),
                metro_stops(zf, stop_ids).with_name("metro_stops"),
            ],
        )
        print(load_info)


if __name__ == "__main__":
    workdir = Path(__file__).parent
    os.chdir(workdir)
    run_pipeline()
