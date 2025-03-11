import datetime
from typing import Literal

from pydantic import BaseModel, Field


class ResponseModel(BaseModel):
    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema, handler):
        # Don't clutter response model docs
        schema = handler(core_schema)
        schema = handler.resolve_ref_schema(schema)
        schema.pop('required', None)
        return schema


class NotFoundResponse(ResponseModel):
    detail: str = Field("Error details")


class Source(ResponseModel):
    id: int = Field(
        description="Bright Sky source ID",
        examples=[
            6007,
        ],
    )
    dwd_station_id: str = Field(
        description="DWD weather station ID",
        examples=[
            "01766",
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wmo_station_id: str = Field(
        description="WMO weather station ID",
        examples=[
            "10315",
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    station_name: str = Field(
        description="DWD weather station name",
        examples=[
            "Münster/Osnabrück",
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    observation_type: Literal[
        'historical',
        'current',
        'synop',
        'forecast',
    ] = Field(
        description="Source type",
    )
    first_record: datetime.datetime = Field(
        description="Timestamp of first available record for this source",
        examples=[
            "2010-01-01T00:00+02:00",
        ],
    )
    last_record: datetime.datetime = Field(
        description="Timestamp of latest available record for this source",
        examples=[
            "2023-08-07T12:40+02:00",
        ],
    )
    lat: float = Field(
        description="Station latitude, in decimal degrees",
        examples=[
            52.1344,
        ],
    )
    lon: float = Field(
        description="Station longitude, in decimal degrees",
        examples=[
            7.6969,
        ],
    )
    height: float = Field(
        description="Station height, in meters",
        examples=[
            47.8,
        ],
    )
    distance: int = Field(
        default=None,
        description="Distance of weather station to the requested `lat` and `lon` (if given), in meters",  # noqa
        examples=[
            16365,
        ],
    )


class BaseWeatherRecord(ResponseModel):
    timestamp: datetime.datetime = Field(
        description="ISO 8601-formatted timestamp of this weather record",
        examples=[
            "2023-08-07T12:30:00+00:00",
        ],
    )
    source_id: int = Field(
        description="Bright Sky source ID for this record",
        examples=[
            238685,
        ],
    )
    cloud_cover: float = Field(
        description="Total cloud cover at timestamp",
        examples=[
            12.1,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    condition: Literal[
        "dry",
        "fog",
        "rain",
        "sleet",
        "snow",
        "hail",
        "thunderstorm",
    ] = Field(
        description="Current weather conditions. Unlike the numerical parameters, this field is not taken as-is from the raw data (because it does not exist), but is calculated from different fields in the raw data as a best effort. Not all values are available for all source types.",  # noqa
        json_schema_extra={
            'nullable': True,
        },
    )
    dew_point: float = Field(
        description="Dew point at timestamp, 2 m above ground",
        examples=[
            -2.5,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    icon: Literal[
        "clear-day",
        "clear-night",
        "partly-cloudy-day",
        "partly-cloudy-night",
        "cloudy",
        "fog",
        "wind",
        "rain",
        "sleet",
        "snow",
        "hail",
        "thunderstorm",
    ] = Field(
        description="Icon alias suitable for the current weather conditions. Unlike the numerical parameters, this field is not taken as-is from the raw data (because it does not exist), but is calculated from different fields in the raw data as a best effort. Not all values are available for all source types.",  # noqa
        json_schema_extra={
            'nullable': True,
        },
    )
    pressure_msl: float = Field(
        description="Atmospheric pressure at timestamp, reduced to mean sea level",  # noqa
        examples=[
            1015.1,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    relative_humidity: int = Field(
        description="Relative humidity at timestamp",
        examples=[
            40,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    temperature: float = Field(
        description="Air temperature at timestamp, 2 m above the ground",
        examples=[
            10.6,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    visibility: int = Field(
        description="Visibility at timestamp",
        examples=[
            50000,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    fallback_source_ids: dict = Field(
        description="Object mapping meteorological parameters to the source IDs of alternative sources that were used to fill up missing values in the main source",  # noqa
        examples=[
            {"pressure_msl": 11831, "wind_speed_30": 11831},
        ],
    )


class WeatherRecord(BaseWeatherRecord):
    precipitation: float = Field(
        description="Total precipitation during previous 60 minutes",
        examples=[
            1.8,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    solar: float = Field(
        description="Solar irradiation during previous 60 minutes",
        examples=[
            0.563,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    sunshine: int = Field(
        description="Sunshine duration during previous 60 minutes",
        examples=[
            58,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_direction: int = Field(
        description="Mean wind direction during previous hour, 10 m above the ground",  # noqa
        examples=[
            70,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_speed: float = Field(
        description="Mean wind speed during previous hour, 10 m above the ground",  # noqa
        examples=[
            12.6,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_gust_direction: int = Field(
        description="Direction of maximum wind gust during previous hour, 10 m above the ground",  # noqa
        examples=[
            50,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_gust_speed: float = Field(
        description="Speed of maximum wind gust during previous hour, 10 m above the ground",  # noqa
        examples=[
            33.5,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    precipitation_probability: int = Field(
        description="Probability of more than 0.1 mm of precipitation in the previous hour (only available in forecasts)",  # noqa
        examples=[
            46,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    precipitation_probability_6h: int = Field(
        description="Probability of more than 0.2 mm of precipitation in the previous 6 hours (only available in forecasts at 0:00, 6:00, 12:00, and 18:00 UTC)",  # noqa
        examples=[
            75,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )


class CurrentWeatherRecord(BaseWeatherRecord):
    precipitation_10: float = Field(
        description="Total precipitation during previous 10 minutes",
        examples=[
            0.8,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    precipitation_30: float = Field(
        description="Total precipitation during previous 30 minutes",
        examples=[
            1.2,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    precipitation_60: float = Field(
        description="Total precipitation during previous 60 minutes",
        examples=[
            1.8,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    solar_10: float = Field(
        description="Solar irradiation during previous 10 minutes",
        examples=[
            0.081,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    solar_30: float = Field(
        description="Solar irradiation during previous 30 minutes",
        examples=[
            0.207,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    solar_60: float = Field(
        description="Solar irradiation during previous 60 minutes",
        examples=[
            0.48,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    sunshine_30: int = Field(
        description="Sunshine duration during previous 30 minutes",
        examples=[
            28,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    sunshine_60: int = Field(
        description="Sunshine duration during previous 60 minutes",
        examples=[
            52,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_direction_10: int = Field(
        description="Mean wind direction during previous 10 minutes, 10 m above the ground",  # noqa
        examples=[
            70,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_direction_30: int = Field(
        description="Mean wind direction during previous 30 minutes, 10 m above the ground",  # noqa
        examples=[
            70,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_direction_60: int = Field(
        description="Mean wind direction during previous 60 minutes, 10 m above the ground",  # noqa
        examples=[
            70,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_speed_10: float = Field(
        description="Mean wind speed during previous 10 minutes, 10 m above the ground",  # noqa
        examples=[
            12.6,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_speed_30: float = Field(
        description="Mean wind speed during previous 30 minutes, 10 m above the ground",  # noqa
        examples=[
            12.6,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_speed_60: float = Field(
        description="Mean wind speed during previous 60 minutes, 10 m above the ground",  # noqa
        examples=[
            12.6,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_gust_direction_10: int = Field(
        description="Direction of maximum wind gust during previous 10 minutes, 10 m above the ground",  # noqa
        examples=[
            50,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_gust_direction_30: int = Field(
        description="Direction of maximum wind gust during previous 30 minutes, 10 m above the ground",  # noqa
        examples=[
            50,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_gust_direction_60: int = Field(
        description="Direction of maximum wind gust during previous 60 minutes, 10 m above the ground",  # noqa
        examples=[
            50,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_gust_speed_10: float = Field(
        description="Speed of maximum wind gust during previous 10 minutes, 10 m above the ground",  # noqa
        examples=[
            33.5,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_gust_speed_30: float = Field(
        description="Speed of maximum wind gust during previous 30 minutes, 10 m above the ground",  # noqa
        examples=[
            33.5,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    wind_gust_speed_60: float = Field(
        description="Speed of maximum wind gust during previous 60 minutes, 10 m above the ground",  # noqa
        examples=[
            33.5,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )


class SynopRecord(CurrentWeatherRecord):
    sunshine_10: int = Field(
        description="Sunshine duration during previous 10 minutes",
        examples=[
            8,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )


class SourcesResponse(ResponseModel):
    sources: list[Source]


class CurrentWeatherResponse(ResponseModel):
    weather: CurrentWeatherRecord
    sources: list[Source]


class WeatherResponse(ResponseModel):
    weather: list[WeatherRecord]
    sources: list[Source]


class SynopResponse(ResponseModel):
    weather: list[SynopRecord]
    sources: list[Source]


class RadarRecord(ResponseModel):
    timestamp: datetime.datetime = Field(
        description="ISO 8601-formatted timestamp of this radar record",
        examples=[
            "2023-08-07T08:00:00+00:00",
        ],
    )
    source: str = Field(
        description="Unique identifier for DWD radar product source of this radar record",  # noqa
        examples=[
            "RADOLAN::RV::2023-08-08T11:45:00+00:00",
        ],
    )
    precipitation_5: str = Field(
        description="Pixelwise 5-minute precipitation data, in units of 0.01 mm / 5 min. Depending on the `format` parameter, this field contains either a two-dimensional array of integers (`plain`), or a base64 string (`bytes` or `compressed`).",  # noqa
        examples=[
            "eF5jGAWjYBTQEQAAA3IAAQ==",
        ],
    )


class RadarResponse(ResponseModel):
    radar: list[RadarRecord]
    geometry: dict = Field(
        description="GeoJSON-formatted bounding box of returned radar data, i.e. lat/lon coordinates of the four corners.",  # noqa
        examples=[
            {
                "type": "Polygon",
                "coordinates": [
                    [7.44365, 52.08712],
                    [7.45668, 51.90644],
                    [7.7487, 51.914],
                    [7.73716, 52.09473],
                ],
            },
        ],
    )
    bbox: list[int] = Field(
        description="Bounding box (top, left, bottom, right) calculated from the supplied position and distance. Only returned if you supplied `lat` and `lon`.",  # noqa
        examples=[
            [100, 100, 300, 300],
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    latlon_position: dict = Field(
        description="Exact x-y-position of the supplied position. Only returned if you supplied `lat` and `lon`.",  # noqa
        examples=[
            {"x": 10.244, "y": 10.088},
        ],
        json_schema_extra={
            'nullable': True,
        },
    )


class Alert(ResponseModel):
    id: int = Field(
        description="Bright Sky-internal ID for this alert",
        examples=[
            279977,
        ],
    )
    alert_id: str = Field(
        description="Unique CAP message identifier",
        examples=[
            "2.49.0.0.276.0.DWD.PVW.1691344680000.2cf9fad6-dc83-44ba-9e88-f2827439da59",  # noqa
        ],
    )
    status: Literal["actual", "test"] = Field(
        description="Alert status",
    )
    effective: datetime.datetime = Field(
        description="Alert issue time",
        examples=[
            "2023-08-06T17:58:00+00:00",
        ],
    )
    onset: datetime.datetime = Field(
        description="Expected event begin time",
        examples=[
            "2023-08-07T08:00:00+00:00",
        ],
    )
    expires: datetime.datetime = Field(
        description="Expected event end time",
        examples=[
            "2023-08-07T19:00:00+00:00",
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    category: Literal["met", "health"] = Field(
        description="Alert category, meteorological message (`met`) or public health related message (`health`)",  # noqa
        json_schema_extra={
            'nullable': True,
        },
    )
    response_type: Literal["prepare", "allclear", "none", "monitor"] = Field(
        description="Code denoting type of action recommended for target audience",  # noqa
        json_schema_extra={
            'nullable': True,
        },
    )
    urgency: Literal["immediate", "future"] = Field(
        description="Alert time frame",
        json_schema_extra={
            'nullable': True,
        },
    )
    severity: Literal["minor", "moderate", "severe", "extreme"] = Field(
        description="Alert severity",
        json_schema_extra={
            'nullable': True,
        },
    )
    certainty: Literal["observed", "likely"] = Field(
        description="Alert certainty",
        json_schema_extra={
            'nullable': True,
        },
    )
    event_code: int = Field(
        description="DWD event code",
        examples=[
            51,
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    event_en: str = Field(
        description="Label for DWD event code (English)",
        examples=[
            "wind gusts",
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    event_de: str = Field(
        description="Label for DWD event code (German)",
        examples=[
            "WINDBÖEN",
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    headline_en: str = Field(
        description="Alert headline (English)",
        examples=[
            "Official WARNING of WIND GUSTS",
        ],
    )
    headline_de: str = Field(
        description="Alert headline (German)",
        examples=[
            "Amtliche WARNUNG vor WINDBÖEN",
        ],
    )
    description_en: str = Field(
        description="Alert description (English)",
        examples=[
            "There is a risk of wind gusts (level 1 of 4).\nMax. gusts: 50-60 km/h; Wind direction: west; Increased gusts: near showers and in exposed locations < 70 km/h",  # noqa
        ],
    )
    description_de: str = Field(
        description="Alert description (German)",
        examples=[
            "Es treten Windböen mit Geschwindigkeiten zwischen 50 km/h (14 m/s, 28 kn, Bft 7) und 60 km/h (17 m/s, 33 kn, Bft 7) aus westlicher Richtung auf. In Schauernähe sowie in exponierten Lagen muss mit Sturmböen bis 70 km/h (20 m/s, 38 kn, Bft 8) gerechnet werden.",  # noqa
        ],
    )
    instruction_en: str = Field(
        description="Additional instructions and safety advice (English)",
        examples=[
            "NOTE: Be aware of the following possible dangers: The downpours can cause temporary traffic disruption.",  # noqa
        ],
        json_schema_extra={
            'nullable': True,
        },
    )
    instruction_de: str = Field(
        description="Additional instructions and safety advice (German)",
        examples=[
            "ACHTUNG! Hinweis auf mögliche Gefahren: Während des Platzregens sind kurzzeitig Verkehrsbehinderungen möglich.",  # noqa
        ],
        json_schema_extra={
            'nullable': True,
        },
    )


class Location(ResponseModel):
    warn_cell_id: int = Field(
        description="Municipality warn cell ID of given location",
        examples=[
            803159016,
        ],
    )
    name: str = Field(
        description="Municipality name",
        examples=[
            "Stadt Göttingen",
        ],
    )
    name_short: str = Field(
        description="Shortened municipality name",
        examples=[
            "Göttingen",
        ],
    )
    district: str = Field(
        description="District name",
        examples=[
            "Göttingen",
        ],
    )
    state: str = Field(
        description="Federal state name",
        examples=[
            "Niedersachsen",
        ],
    )
    state_short: str = Field(
        description="Shortened federal state name",
        examples=[
            "NI",
        ],
    )


class AlertsResponse(ResponseModel):
    alerts: list[Alert]
    location: Location = Field(
        json_schema_extra={
            'nullable': True,
        },
    )
