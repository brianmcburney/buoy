import datetime
from dataclasses import dataclass
from typing import Optional


@dataclass
class Station:
    id: int
    name: str
    rss: str
    latitude: float
    longitude: float


@dataclass
class StationReport:
    station_id: int
    timestamp: datetime.datetime
    wave_height: Optional[float] = None
    wave_dominant_period: Optional[float] = None
    wave_average_period: Optional[float] = None
    wave_mean_degrees: Optional[float] = None
    water_temperature: Optional[float] = None
