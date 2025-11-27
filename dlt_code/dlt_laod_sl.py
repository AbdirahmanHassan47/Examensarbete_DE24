import dlt
import requests
from datetime import datetime
from pathlib import Path
import os

BASE_URL = "https://transport.integration.sl.se/v1"


def _get_departures_for_site(site_id: int):
    url = f"{BASE_URL}/sites/{site_id}/departures"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


@dlt.resource(write_disposition="append")
def sl_departures_resource(site_ids: list[int]):
    """
    Hämtar avgångar för en lista av site_ids.
    En "yield" = en avgång (flattenad).
    """
    for site_id in site_ids:
        data = _get_departures_for_site(site_id)

        # du får anpassa flatten beroende på exakt JSON-struktur
        departures = data.get("departures", [])
        for dep in departures:
            # enrich med metadata
            dep["_site_id"] = site_id
            dep["_ingested_at"] = datetime.utcnow().isoformat()
            yield dep


def run_pipeline(table_name: str = "sl_departures_raw"):
    pipeline = dlt.pipeline(
        pipeline_name="sl_departures",
        destination="snowflake",
        dataset_name="staging",   # schema TRAFIK_DATA.STAGING
    )

    site_ids = [
        9192,  # Slussen
        9302,  # T-centralen (exempel)
    ]

    load_info = pipeline.run(
        sl_departures_resource(site_ids=site_ids),
        table_name=table_name,
    )

    print(load_info)


if __name__ == "__main__":
    # se till att jobba i samma dir som scriptet (så .dlt/ hittas)
    workdir = Path(__file__).parent
    os.chdir(workdir)
    run_pipeline()
