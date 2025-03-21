import datetime
import re
from contextlib import suppress
from typing import Annotated, Literal

from dateutil.tz import gettz
from fastapi import Query
from pydantic import BaseModel, Field, field_validator, model_validator


class LatLon(BaseModel):
    lat: float = Field(
        ge=-90,
        le=90,
        default=None,
        description="Latitude in decimal degrees.",
        examples=[
            52.52,
            51.55,
        ],
    )
    lon: float = Field(
        ge=-180,
        le=180,
        default=None,
        description="Longitude in decimal degrees.",
        examples=[
            13.4,
            9.9,
        ],
    )

    @model_validator(mode='after')
    def validate_both_or_no_lat_lon(self):
        if (self.lat is None) != (self.lon is None):
            raise ValueError("Supply both lat and lon, or none of the two")
        return self


class MaxDist(BaseModel):
    max_dist: int = Field(
        ge=0,
        le=500000,
        default=50000,
        description="Maximum distance of record location from the location given by `lat` and `lon`, in meters. Only has an effect when using `lat` and `lon`.",  # noqa
        examples=[
            10000,
        ],
    )


def _split(value, converter=str, sep=','):
    if len(value) > 1:
        # e.g. ?param=1&param=2
        return value
    # e.g. ?param=1,2
    def _convert(x):
        with suppress(Exception):
            return converter(x)
        raise ValueError(f"'{x}' is not a valid {converter.__name__}")
    return [_convert(x) for x in value[0].split(sep) if x]


class StationIDs(BaseModel):
    dwd_station_ids: Annotated[
        list[str],
        Query(alias='dwd_station_id'),
    ] = Field(
        default=None,
        description="DWD station ID, typically five alphanumeric characters. You can supply multiple station IDs separated by commas, ordered from highest to lowest priority.",  # noqa
        examples=[
            "01766",
            "00420,00053,00400",
        ],
    )
    wmo_station_ids: Annotated[
        list[str],
        Query(alias='wmo_station_id'),
    ] = Field(
        default=None,
        description="WMO station ID, typically five alphanumeric characters. You can supply multiple station IDs separated by commas, ordered from highest to lowest priority.",  # noqa
        examples=[
            "10315",
            "G005,F451,10389",
        ],
    )

    @field_validator('dwd_station_ids', mode='before')
    @classmethod
    def validate_dwd_station_ids(cls, value):
        return _split(value)

    @field_validator('wmo_station_ids', mode='before')
    @classmethod
    def validate_wmo_station_ids(cls, value):
        return _split(value)


class SourceIDs(BaseModel):
    source_ids: Annotated[
        list[int],
        Query(alias='source_id'),
    ] = Field(
        default=None,
        description="Bright Sky source ID, as retrieved from the [`/sources` endpoint](/operations/getSources). You can supply multiple source IDs separated by commas, ordered from highest to lowest priority.",  # noqa
        examples=[
            "1234",
            "1234,2345",
        ],
    )

    @field_validator('source_ids', mode='before')
    @classmethod
    def validate_source_ids(cls, value):
        return _split(value, converter=int)


class DateRange(BaseModel):
    date: datetime.datetime = Field(
        description="Timestamp of first weather record (or forecast) to retrieve, in ISO 8601 format. May contain time and/or UTC offset.",  # noqa
        examples=[
            "2023-08-07",
            "2023-08-07T08:00+02:00",
        ],
    )
    last_date: datetime.datetime = Field(
        default=None,
        description="Timestamp of last weather record (or forecast) to retrieve, in ISO 8601 format. Will default to `date + 1 day`.",  # noqa
        examples=[
            "2023-08-08",
            "2023-08-07T23:00+02:00",
        ],
    )

    @field_validator('date', 'last_date', mode='before')
    def fix_unescaped_offset(cls, value):
        # Handle " 02:00" (i.e. space in raw URL) instead of "%2B02:00"
        return re.sub(r' (\d{2}:\d{2})$', r'+\1', value)

    @model_validator(mode='after')
    def set_default_last_date(self):
        if self.last_date is None:
            self.last_date = self.date + datetime.timedelta(days=1)
        return self

    @model_validator(mode='after')
    def ensure_tzinfo(self):
        default = getattr(self, 'timezone', None) or datetime.UTC
        if not self.date.tzinfo:
            self.date = self.date.replace(tzinfo=default)
        if not self.last_date.tzinfo:
            self.last_date = self.last_date.replace(tzinfo=default)
        return self


class Timezone(BaseModel):
    timezone: Annotated[
        str,
        Query(alias='tz'),
    ] = Field(
        default=None,
        description="Timezone in which record timestamps will be presented, as <a href=\"https://en.wikipedia.org/wiki/List_of_tz_database_time_zones\">tz database name</a>. Will also be used as timezone when parsing `date` and `last_date`, unless these have explicit UTC offsets. If omitted but `date` has an explicit UTC offset, that offset will be used as timezone. Otherwise will default to UTC.",  # noqa
        examples=[
            "Europe/Berlin",
            "Australia/Darwin",
            "Etc/UTC",
        ],
    )

    @field_validator('timezone', mode='after')
    @classmethod
    def validate_timezone(cls, value):
        if value and not gettz(value):
            raise ValueError(f"Unknown timezone: {value}")
        return gettz(value)

    @model_validator(mode='after')
    def set_default_timezone_from_date(self):
        if self.timezone:
            return self
        if getattr(self, 'date', None) and self.date.tzinfo != datetime.UTC:
            self.timezone = self.date.tzinfo
        return self


class Units(BaseModel):
    units: Literal['dwd', 'si'] = Field(
        default='dwd',
        description="""\
Physical units in which meteorological parameters will be returned. Set to `si` to use <a href="https://en.wikipedia.org/wiki/International_System_of_Units">SI units</a> (except for precipitation, which is always measured in millimeters). The default `dwd` option uses a set of units that is more common in meteorological applications and civil use:
<table>
  <tr><td></td><td>DWD</td><td>SI</td></tr>
  <tr><td>Cloud cover</td><td>%</td><td>%</td></tr>
  <tr><td>Dew point</td><td>°C</td><td>K</td></tr>
  <tr><td>Precipitation</td><td>mm</td><td><s>kg / m²</s> <strong>mm</strong></td></tr>
  <tr><td>Precipitation probability</td><td>%</td><td>%</td></tr>
  <tr><td>Pressure</td><td>hPa</td><td>Pa</td></tr>
  <tr><td>Relative humidity</td><td>%</td><td>%</td></tr>
  <tr><td>Solar irradiation</td><td>kWh / m²</td><td>J / m²</td></tr>
  <tr><td>Sunshine</td><td>min</td><td>s</td></tr>
  <tr><td>Temperature</td><td>°C</td><td>K</td></tr>
  <tr><td>Visibility</td><td>m</td><td>m</td></tr>
  <tr><td>Wind (gust) direction</td><td>°</td><td>°</td></tr>
  <tr><td>Wind (gust) speed</td><td>km / h</td><td>m / s</td></tr>
</table>""",  # noqa
    )


class WarnCell(BaseModel):
    warn_cell_id: int = Field(
        default=None,
        description="Municipality warn cell ID.",
        examples=[
            803159016,
            705515101,
        ],
    )


class RadarDateRange(BaseModel):
    date: datetime.datetime = Field(
        default=None,
        description="Timestamp of first record to retrieve, in ISO 8601 format. May contain time and/or UTC offset. (_Defaults to 1 hour before latest measurement._)",  # noqa
        examples=[
            "2023-08-07",
            "2023-08-07T19:00+02:00",
        ],
    )
    last_date: datetime.datetime = Field(
        default=None,
        description="Timestamp of last record to retrieve, in ISO 8601 format. May contain time and/or UTC offset. (_Defaults to 2 hours after `date`._)",  # noqa
        examples=[
            "2023-08-08",
            "2023-08-07T23:00+02:00",
        ],
    )

    @field_validator('date', 'last_date', mode='before')
    def fix_unescaped_offset(cls, value):
        # Handle " 02:00" (i.e. space in raw URL) instead of "%2B02:00"
        return re.sub(r' (\d{2}:\d{2})$', r'+\1', value)

    @model_validator(mode='after')
    def set_default_last_date(self):
        if self.last_date is None and self.date is not None:
            self.last_date = self.date + datetime.timedelta(hours=2)
        return self

    @field_validator('date', 'last_date', mode='after')
    @classmethod
    def ensure_tzinfo(cls, value):
        if value is None or value.tzinfo:
            return value
        return value.replace(tzinfo=datetime.UTC)


class RadarBoundingBox(BaseModel):
    bbox: list[int] = Field(
        default=None,
        description="Bounding box (top, left, bottom, right) **in pixels**, edges are inclusive. (_Defaults to full 1200x1100 grid._)",  # noqa
        examples=[
            "100,100,300,300",
        ],
    )
    distance: int = Field(
        default=200000,
        description="Alternative way to set a bounding box, must be used together with `lat` and `lon`. Data will reach `distance` meters to each side of this location, but is possibly cut off at the edges of the radar grid.",  # noqa
        examples=[
            100000,
        ],
    )

    @field_validator('bbox', mode='before')
    @classmethod
    def validate_bbox(cls, value):
        bbox = _split(value, converter=int)
        if len(bbox) != 4:
            raise ValueError(
                "The 'bbox' parameter must be a comma-separated list of four "
                "integers: top, left, bottom, right (edges are inclusive)."
            )
        return bbox


class RadarFormat(BaseModel):
    format: Literal['compressed', 'bytes', 'plain'] = Field(
        default='compressed',
        description="""
Determines how the precipitation data is encoded into the `precipitation_5` field:
* `compressed`: base64-encoded, zlib-compressed bytestring of 2-byte integers
* `bytes`: base64-encoded bytestring of 2-byte integers
* `plain`: Nested array of integers
        """,  # noqa
    )


class SourcesParams(
    Timezone,
    SourceIDs,
    StationIDs,
    MaxDist,
    LatLon,
):
    @model_validator(mode='after')
    def validate_at_least_one_option(self):
        if self.lat is not None and self.lon is not None:
            return self
        elif any([
            self.dwd_station_ids,
            self.wmo_station_ids,
            self.source_ids,
        ]):
            return self
        raise ValueError(
            "Please supply lat & lon, or dwd_station_id, or wmo_station_id, "
            "or source_id",
        )


class CurrentWeatherParams(
    Units,
    SourcesParams,
):
    pass


class WeatherParams(
    Units,
    SourcesParams,
    DateRange,
):
    pass


class SynopParams(
    Units,
    Timezone,
    SourceIDs,
    StationIDs,
    DateRange,
):
    @model_validator(mode='after')
    def validate_at_least_one_option(self):
        if any([
            self.dwd_station_ids,
            self.wmo_station_ids,
            self.source_ids,
        ]):
            return self
        raise ValueError(
            "Please dwd_station_id or wmo_station_id or source_id",
        )


class RadarParams(
    Timezone,
    RadarFormat,
    RadarDateRange,
    LatLon,
    RadarBoundingBox,
):
    pass


class AlertsParams(
    Timezone,
    WarnCell,
    LatLon,
):
    pass
