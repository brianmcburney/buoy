import asyncio
import datetime
import hashlib
import logging
import os
import pickle
import posixpath
import re
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from types import TracebackType
from typing import List, Optional, Type

import aiofiles
import aiohttp
import bs4


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


BeautifulSoup = partial(bs4.BeautifulSoup, features="lxml")


@dataclass
class Station:
    id: int
    name: str
    rss: str
    latitude: float
    longitude: float


@dataclass
class StationReport:
    timestamp: datetime.datetime
    wave_height: Optional[float] = None
    wave_dominant_period: Optional[float] = None
    wave_average_period: Optional[float] = None
    wave_mean_degrees: Optional[float] = None
    water_temperature: Optional[float] = None


class NOAAClient:
    def __init__(self) -> None:
        self.base_url = "https://www.ndbc.noaa.gov"
        self.session = aiohttp.ClientSession()

    async def __aenter__(self) -> "NOAAClient":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc: Optional[Exception],
        tb: Optional[TracebackType],
    ) -> None:
        await self.session.close()

    def _url(self, path: List[str]) -> str:
        return posixpath.join(self.base_url, *path)

    async def _get(self, url: str, params: dict = {}) -> Optional[str]:
        retval = None
        cache_key = hashlib.sha1(f"{url}.{params}".encode()).hexdigest()
        cache_file = posixpath.join(".", "cache", f"{cache_key}.html")

        try:
            async with aiofiles.open(cache_file, "r") as f:
                return await f.read()
        except FileNotFoundError as e:
            logger.info(e)

        response = await self.session.get(url, params=params)
        if response.status == 200:
            logger.info(f"{response.real_url}")
            retval = await response.text()
            async with aiofiles.open(cache_file, "w") as f:
                await f.write(retval)

        return retval

    async def station(self, station_id: int) -> Optional[Station]:
        station = None

        local_file = Path(".") / "stations" / f"{station_id}.pickle"
        try:
            async with aiofiles.open(local_file, "rb") as f:
                data = await f.read()
                station = pickle.loads(data)
        except FileNotFoundError:
            url = self._url(["station_page.php"])
            html = await self._get(url, {"station": station_id})
            if html:
                soup = BeautifulSoup(html)

                for a in soup.findAll("a", href=True):
                    if a["href"] == f"/data/latest_obs/{station_id}.rss":
                        rss = self._url([a["href"].lstrip("/")])
                        name = a.parent.get_text()
                        station_data = soup.find(
                            "div", {"id": "stn_metadata"}
                        ).get_text()
                        lat_long = re.search(
                            r"([\d\.]+ [NS]) ([\d\.]+ [EW])", station_data
                        )
                        if lat_long:
                            station = Station(
                                station_id,
                                name,
                                rss,
                                lat_long.group(1),
                                lat_long.group(2),
                            )
                            async with aiofiles.open(local_file, "wb") as f:
                                await f.write(pickle.dumps(station))

        return station

    async def station_report(self, station_id: int) -> Optional[StationReport]:
        url = self._url(["station_page.php"])
        html = await self._get(url, {"station": station_id})
        if not html:
            return None

        soup = BeautifulSoup(html)
        caption = soup.find("caption", {"class": "titleDataHeader"})
        if not caption:
            return None

        m = re.search(r"(\d{4} GMT .+):$", caption.getText())
        if not m:
            return None

        timestamp = datetime.datetime.strptime(m.group(1), "%H%M %Z on %m/%d/%Y")
        report = StationReport(timestamp)

        table = caption.parent
        for row in table.findAll("tr"):
            cells = row.findAll("td")
            if len(cells) < 3:
                continue

            key = cells[1].getText()
            value = cells[2].getText().split()

            if re.search(r"^Wave Height", key):
                report.wave_height = value
            elif re.search(r"^Dominant Wave Period", key):
                report.wave_dominant_period = value
            elif re.search(r"^Average Period", key):
                report.wave_average_period = value
            elif re.search(r"^Mean Wave Direction", key):
                m = re.search(r"(\d+ deg)", cells[2].getText())
                if m:
                    report.wave_mean_degrees = m.group(1).split()
            elif re.search(r"^Water Temperature", key):
                report.water_temperature = value

        try:
            local_path = Path("reports") / str(station_id)
            local_file = local_path / f"{timestamp.timestamp()}.pickle"
            os.makedirs(local_path)
        except FileExistsError:
            pass

        async with aiofiles.open(local_file, "wb") as f:
            await f.write(pickle.dumps(report))

        return report

    async def station_list(self) -> List[int]:
        url = self._url(["to_station.shtml"])
        html = await self._get(url)
        soup = BeautifulSoup(html)

        stations = []
        for a in soup.findAll("a", href=True):
            m = re.search(r"station_page.php\?station=(\d+)$", a["href"])
            if m:
                try:
                    station_id = int(m.group(1))
                    stations.append(station_id)
                except ValueError:
                    continue

        return stations

    async def search(
        self, latitude: float, longitude: float, distance: int = 250
    ) -> Optional[str]:
        url = posixpath.join(self.base_url, "radial_search.php")
        params = {"lat1": latitude, "lon1": longitude, "dist": distance, "uom": "E"}
        return await self._get(url, params)


async def run() -> Optional[dict]:
    async with NOAAClient() as client:
        stations = await client.station_list()
        tasks = [client.station_report(station_id) for station_id in stations]
        result = await asyncio.gather(*tasks)
        reports = {
            station_id: report for station_id, report in zip(stations, result)
        }  # noqa

        # tasks = [client.station(station_id) for station_id in stations]
        # result = [x for x in await asyncio.gather(*tasks) if x is not None]
        return reports
        # result = await client.search('32.868N', '117.267W')
        # logger.info(result)


if __name__ == "__main__":
    logger.info("Starting up")
    asyncio.run(run())

# https://www.ndbc.noaa.gov/radial_search.php?lat1=32.868N&lon1=117.267W&uom=E&dist=250
