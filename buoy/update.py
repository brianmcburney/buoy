import asyncio
import datetime
import json
import os
from dataclasses import asdict
from typing import Any, Dict

import aiobotocore

from buoy.client import NOAAClient


BUCKET = "buoy-dev"
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_KEY")


def _json_serialize(obj: Any) -> Any:
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()


async def upload_data(objects: Dict[str, object]) -> None:
    session = aiobotocore.get_session(loop=asyncio.get_event_loop())
    async with session.create_client(
        "s3",
        region_name="us-west-2",
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
    ) as client:
        tasks = [
            client.put_object(
                Bucket=BUCKET, Key=key, Body=json.dumps(data, default=_json_serialize)
            )
            for key, data in objects.items()
        ]
        await asyncio.gather(*tasks)


async def update_stations() -> None:
    async with NOAAClient() as client:
        stations = await client.station_list()
        tasks = [client.station(station_id) for station_id in stations]
        stations = [x for x in await asyncio.gather(*tasks) if x is not None]
        await upload_data({"stations.json": [asdict(x) for x in stations]})


async def update_reports() -> None:
    async with NOAAClient() as client:
        stations = await client.station_list()
        tasks = [client.station_report(station_id) for station_id in stations]
        reports = [x for x in await asyncio.gather(*tasks) if x is not None]
        await upload_data(
            {f"report/{report.station_id}.json": asdict(report) for report in reports}
        )


if __name__ == "__main__":
    asyncio.run(update_reports())
