import base64
import contextlib
from multiprocessing import cpu_count
from pathlib import Path
from typing import Any, Annotated

import asyncpg
import orjson
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

import brightsky
from brightsky import query
from brightsky.enhancements import enhance
from brightsky.settings import settings

from .models import (
    AlertsResponse,
    CurrentWeatherResponse,
    NotFoundResponse,
    RadarResponse,
    SourcesResponse,
    SynopResponse,
    WeatherResponse,
)
from .params import (
    AlertsParams,
    CurrentWeatherParams,
    RadarParams,
    SourcesParams,
    SynopParams,
    WeatherParams,
)


with open(Path(__file__).parent / 'intro.md') as f:
    DESCRIPTION = f.read()


OPENAPI_METADATA = {
    'title': 'Bright Sky',
    'description': DESCRIPTION,
    'version': brightsky.__version__,
    'contact': {
        'name': 'Jakob de Maeyer',
        'email': 'jakob@brightsky.dev',
        'url': 'https://brightsky.dev/',
    },
    'license_info': {
        'name': 'MIT License',
        'identifier': 'MIT',
    },
    'servers': [
        {'url': settings.SERVER_URL},
    ],
}


ctx = {}


async def init_connection(conn):
    await conn.set_type_codec(
        'real',
        encoder=str,
        decoder=float,
        schema='pg_catalog',
    )


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    async with asyncpg.create_pool(
        dsn=settings.DATABASE_URL,
        min_size=1,
        max_size=cpu_count(),
        init=init_connection,
    ) as pool:
        ctx['pool'] = pool
        yield
        del ctx['pool']


def make_app():
    app = FastAPI(
        lifespan=lifespan,
        **OPENAPI_METADATA,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=(
            ["*"]
            if settings.CORS_ALLOW_ALL_ORIGINS
            else settings.CORS_ALLOWED_ORIGINS
        ),
        allow_headers=(
            ["*"]
            if settings.CORS_ALLOW_ALL_HEADERS
            else settings.CORS_ALLOWED_HEADERS
        ),
    )
    return app


app = make_app()


@app.exception_handler(query.NoData)
async def not_found(request, exc) -> NotFoundResponse:
    return ORJSONResponse(
        status_code=404,
        content={'detail': str(exc)},
    )


common_responses = {
    404: {'model': NotFoundResponse},
}


@app.get('/', include_in_schema=False)
async def status():
    return ORJSONResponse({
        'name': 'brightsky',
        'version': brightsky.__version__,
        'status': 'ok',
    })


@app.get(
    '/sources',
    operation_id='getSources',
    summary='Weather sources (stations)',
    tags=['Internals'],
    responses=common_responses,
)
async def sources(
    q: Annotated[SourcesParams, Query()],
) -> SourcesResponse:
    """
    Return a list of all Bright Sky sources matching the given location
    criteria, ordered by distance.

    You must supply both `lat` and `lon` _or_ one of `dwd_station_id`,
    `wmo_station_id`, or `source_id`.
    """
    result = await query.sources(
        ctx['pool'],
        lat=q.lat,
        lon=q.lon,
        max_dist=q.max_dist,
        dwd_station_ids=q.dwd_station_ids,
        wmo_station_ids=q.wmo_station_ids,
        source_ids=q.source_ids,
        ignore_type=True,
    )
    enhance(result, timezone=q.timezone)
    return ORJSONResponse(result)


@app.get(
    '/current_weather',
    operation_id='getCurrentWeather',
    summary='Current weather',
    responses=common_responses,
)
async def current_weather(
    q: Annotated[CurrentWeatherParams, Query()],
) -> CurrentWeatherResponse:
    """
    Returns current weather for a given location.

    To set the location for which to retrieve weather, you must supply both
    `lat` and `lon` _or_ one of `dwd_station_id`, `wmo_station_id`, or
    `source_id`.

    This endpoint is different from the other weather endpoints in that it does
    not directly correspond to any of the data available from the DWD Open Data
    server. Instead, it is a best-effort solution to reflect current weather
    conditions by compiling [SYNOP observations](/operations/getSynop) from the
    past one and a half hours.
    """
    result = await query.current_weather(
        ctx['pool'],
        lat=q.lat,
        lon=q.lon,
        max_dist=q.max_dist,
        dwd_station_ids=q.dwd_station_ids,
        wmo_station_ids=q.wmo_station_ids,
        source_ids=q.source_ids,
    )
    enhance(result, timezone=q.timezone, units=q.units)
    return ORJSONResponse(result)


@app.get(
    '/weather',
    operation_id='getWeather',
    summary='Hourly weather (including forecasts)',
    responses=common_responses,
)
async def weather(
    q: Annotated[WeatherParams, Query()],
) -> WeatherResponse:
    """
    Returns a list of hourly weather records (and/or forecasts) for the time
    range given by `date` and `last_date`.

    To set the location for which to retrieve records (and/or forecasts), you
    must supply both `lat` and `lon` _or_ one of `dwd_station_id`,
    `wmo_station_id`, or `source_id`.
    """
    result = await query.weather(
        ctx['pool'],
        date=q.date,
        last_date=q.last_date,
        lat=q.lat,
        lon=q.lon,
        max_dist=q.max_dist,
        dwd_station_ids=q.dwd_station_ids,
        wmo_station_ids=q.wmo_station_ids,
        source_ids=q.source_ids,
    )
    enhance(result, timezone=q.timezone, units=q.units)
    return ORJSONResponse(result)


@app.get(
    '/synop',
    operation_id='getSynop',
    summary='Raw SYNOP observations',
    tags=['Internals'],
    responses=common_responses,
)
async def synop(
    q: Annotated[SynopParams, Query()],
) -> SynopResponse:
    """
    Returns a list of ten-minutely SYNOP observations for the time range given
    by `date` and `last_date`. Note that Bright Sky only stores SYNOP
    observations from the past 30 hours.

    To set the weather station for which to retrieve records, you must supply
    one of `dwd_station_id`, `wmo_station_id`, or `source_id`. The `/synop`
    endpoint does not support `lat` and `lon`; use the
    [`/sources` endpoint](/operations/getSources) if you need to retrieve a
    SYNOP station ID close to a given location.

    SYNOP observations are stored as they were reported, which in particular
    implies that many parameters are only available at certain timestamps. For
    example, most stations report `sunshine_60` only on the full hour, and
    `sunshine_30` only at 30 minutes past the full hour (i.e. also not on the
    full hour). Check out the
    [`/current_weather` endpoint](/operations/getCurrentWeather) for an
    opinionated compilation of recent SYNOP records into a single "current
    weather" record.
    """
    result = await query.synop(
        ctx['pool'],
        date=q.date,
        last_date=q.last_date,
        dwd_station_ids=q.dwd_station_ids,
        wmo_station_ids=q.wmo_station_ids,
        source_ids=q.source_ids,
    )
    enhance(result, timezone=q.timezone, units=q.units)
    return ORJSONResponse(result)


class BytesORJSONResponse(ORJSONResponse):
    @staticmethod
    def encode_bytes(o):
        if isinstance(o, (bytes, memoryview)):
            return base64.b64encode(o).decode('ascii')
        raise TypeError

    def render(self, content: Any) -> bytes:
        return orjson.dumps(
            content,
            default=self.encode_bytes,
            option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY,
        )


@app.get(
    '/radar',
    operation_id='getRadar',
    summary='Radar',
    responses=common_responses,
)
async def radar(
    q: Annotated[RadarParams, Query()],
    request: Request,
    response: Response,
) -> RadarResponse:
    """
    Returns radar rainfall data with 1 km spatial and 5 minute temporal
    resolution, including a forecast for the next two hours.

    Radar data is recorded on a 1200 km (North-South) x 1100 km (East-West)
    grid, with each pixel representing 1 km². **That's quite a lot of data, so
    use `lat`/`lon` or `bbox` whenever you can (see below).** Past radar
    records are kept for six hours.

    Bright Sky can return the data in a few formats. Use the default
    `compressed` format if possible – this'll get you the fastest response
    times by far and reduce load on the server. If you have a small-ish
    bounding box (e.g. 250 x 250 pixels), using the `plain` format should be
    fine.

    ### Quickstart

    This request will get you radar data near Münster, reaching 200 km to the
    East/West/North/South, as a two-dimensional grid of integers:

    [`https://api.brightsky.dev/radar?lat=52&lon=7.6&format=plain`](https://api.brightsky.dev/radar?lat=52&lon=7.6&format=plain)

    ### Content

    * The grid is a polar stereographic projection of Germany and the regions
      bordering it. This is different from the mercator projection used for
      most consumer-facing maps like OpenStreetMap or Google Maps, and
      overlaying the radar data onto such a map without conversion
      (reprojection) will be inaccurate! Check out our [radar
      demo](https://brightsky.dev/demo/radar/) for an example of correctly
      reprojecting the radar data using OpenLayers. Alternatively, take a look
      at the `dwd:RV-Produkt` layer on the [DWD's open
      GeoServer](https://maps.dwd.de/geoserver/web/wicket/bookmarkable/org.geoserver.web.demo.MapPreviewPage)
      for ready-made tiles you can overlay onto a map.
    * The [proj-string](https://proj.org/en/9.2/usage/quickstart.html) for the
      grid projection is `+proj=stere +lat_0=90 +lat_ts=60 +lon_0=10 +a=6378137
      +b=6356752.3142451802 +no_defs +x_0=543196.83521776402
      +y_0=3622588.8619310018`. The radar pixels range from `-500` (left) to
      `1099500` (right) on the x-axis, and from `500` (top) to `-1199500`
      (bottom) on the y-axis, each radar pixel a size of `1000x1000` (1 km²).
    * The DWD data does not cover the whole grid! Many areas near the edges
      will always be `0`.
    * Values represent 0.01 mm / 5 min. I.e., if a pixel has a value of `45`,
      then 0.45 mm of precipitation fell in the corresponding square kilometer
      in the past five minutes.
    * The four corners of the grid are as follows:
      * Northwest: Latitude `55.86208711`, Longitude `1.463301510`
      * Northeast: Latitude `55.84543856`, Longitude `18.73161645`
      * Southeast: Latitude `45.68460578`, Longitude `16.58086935`
      * Southwest: Latitude `45.69642538`, Longitude `3.566994635`

    You can find details and more information in the [DWD's `RV product info`
    (German
    only)](https://www.dwd.de/DE/leistungen/radarprodukte/formatbeschreibung_rv.pdf?__blob=publicationFile&v=3).
    Below is an example visualization of the rainfall radar data taken from
    this document, using the correct projection and showing the radar coverage:

    ![image](https://github.com/jdemaeyer/brightsky/assets/10531844/09f9bb5f-088a-417e-8a0c-ea5a20fe0969)

    ### Code examples

    > The radar data is quite big (naively unpacking the default 25-frames
    > response into Python integer arrays will eat roughly 125 MB of memory),
    > so use `bbox` whenever you can.

    #### `compressed` format

    With Javascript using [`pako`](https://github.com/nodeca/pako):

    ```js
    fetch(
      'https://api.brightsky.dev/radar'
    ).then((resp) => resp.json()
    ).then((respData) => {
      const raw = respData.radar[0].precipitation_5;
      const compressed = Uint8Array.from(atob(raw), c => c.charCodeAt(0));
      const rawBytes = pako.inflate(compressed).buffer;
      const precipitation = new Uint16Array(rawBytes);
    });
    ```

    With Python using `numpy`:

    ```python
    import base64
    import zlib

    import numpy as np
    import requests

    resp = requests.get('https://api.brightsky.dev/radar')
    raw = resp.json()['radar'][0]['precipitation_5']
    raw_bytes = zlib.decompress(base64.b64decode(raw))

    data = np.frombuffer(
        raw_bytes,
        dtype='i2',
    ).reshape(
        # Adjust `1200` and `1100` to the height/width of your bbox
        (1200, 1100),
    )
    ```

    With Python using the standard library's `array`:
    ```python
    import array

    # [... load raw_bytes as above ...]

    data = array.array('H')
    data.frombytes(raw_bytes)
    data = [
        # Adjust `1200` and `1100` to the height/width of your bbox
        data[row*1100:(row+1)*1100]
        for row in range(1200)
    ]
    ```

    Simple plot using `matplotlib`:
    ```python
    import matplotlib.pyplot as plt

    # [... load data as above ...]

    plt.imshow(data, vmax=50)
    plt.show()
    ```

    #### `bytes` format

    Same as for `compressed`, but add `?format=bytes` to the URL and remove the
    call to `zlib.decompress`, using just `raw_bytes = base64.b64decode(raw)`
    instead.

    #### `plain` format

    This is obviously a lot simpler than the `compressed` format. It is,
    however, also a lot slower. Nonetheless, if you have a small-ish `bbox` the
    performance difference becomes manageable, so just using the `plain` format
    and not having to deal with unpacking logic can be a good option in this
    case.

    With Python:
    ```python
    import requests

    resp = requests.get('https://api.brightsky.dev/radar?format=plain')
    data = resp.json()['radar'][0]['precipitation_5']
    ```

    ### Additional resources

    * [Source for our radar demo, including reprojecton via OpenLayers](https://github.com/jdemaeyer/brightsky/blob/master/docs/demo/radar/index.html)
    * [Raw data on the Open Data Server](https://opendata.dwd.de/weather/radar/composite/rv/)
    * [Details on the `RV` product (German)](https://www.dwd.de/DE/leistungen/radarprodukte/formatbeschreibung_rv.pdf?__blob=publicationFile&v=3)
    * [Visualization of current rainfall radar](https://www.dwd.de/DE/leistungen/radarbild_film/radarbild_film.html)
    * [General info on DWD radar products (German)](https://www.dwd.de/DE/leistungen/radarprodukte/radarprodukte.html)
    * [Radar status (German)](https://www.dwd.de/DE/leistungen/radarniederschlag/rn_info/home_freie_radarstatus_kartendaten.html?nn=16102)
    * [DWD notifications for radar products (German)](https://www.dwd.de/DE/leistungen/radolan/radolan_info/radolan_informationen.html?nn=16102)
    """
    if q.format == 'compressed':
        # Prevent traefik from gzipping the pre-compressed content
        response.headers['Content-Encoding'] = 'identity'
    else:
        allowed_encodings = ['br', 'zstd', 'gzip']
        accepted_encoding = request.headers.get('accept-encoding', '')
        if not any(e in accepted_encoding for e in allowed_encodings):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Requests to the radar endpoint with format 'plain' or "
                    "'bytes' must accept br, zstd, or gzip encoding"
                ),
            )
    result = await query.radar(
        ctx['pool'],
        date=q.date,
        last_date=q.last_date,
        lat=q.lat,
        lon=q.lon,
        distance=q.distance,
        fmt=q.format,
        bbox=q.bbox,
    )
    enhance(result, timezone=q.timezone)
    return BytesORJSONResponse(result)


@app.get(
    '/alerts',
    operation_id='getAlerts',
    summary='Alerts',
    responses=common_responses,
)
async def alerts(
    q: Annotated[AlertsParams, Query()],
) -> AlertsResponse:
    """
    Returns a list of weather alerts for the given location, or all weather
    alerts if no location given.

    If you supply either `warn_cell_id` or both `lat` and `lon`, Bright Sky
    will return additional information on that cell in the `location` field.
    Warn cell IDs are municipality (_Gemeinde_) cell IDs.

    ### Notes

    * The DWD divides Germany's area into roughly 11,000 "cells" based on
      municipalities (_Gemeinden_), and issues warnings for each of these
      cells. Most warnings apply to many cells.
    * Bright Sky will supply information on the cell that a given lat/lon
      belongs to in the `location` field.
    * There is also a set of roughly 400 cells based on districts
      (_Landkreise_) that is not supported by Bright Sky.
    * The complete list of cells can be found on the DWD GeoServer (see below).

    ### Additional resources

    * [Live demo of a simple interactive alerts map](https://brightsky.dev/demo/alerts/)
    * [Source for alerts map demo](https://github.com/jdemaeyer/brightsky/blob/master/docs/demo/alerts/index.html)
    * [Map view of all current alerts by the DWD](https://www.dwd.de/DE/wetter/warnungen_gemeinden/warnWetter_node.html)
    * [List of municipality cells](https://maps.dwd.de/geoserver/wfs?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=Warngebiete_Gemeinden&OUTPUTFORMAT=json)
    * [List of district cells (*not used by Bright Sky!*)](https://maps.dwd.de/geoserver/wfs?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=Warngebiete_Kreise&OUTPUTFORMAT=json)
    * [Raw data on the Open Data Server](https://opendata.dwd.de/weather/alerts/cap/COMMUNEUNION_DWD_STAT/)
    * [DWD Documentation on alert fields and their allowed contents (English)](https://www.dwd.de/DE/leistungen/opendata/help/warnungen/cap_dwd_profile_en_pdf_2_1_13.pdf?__blob=publicationFile&v=3)
    * [DWD Documentation on alert fields and their allowed contents (German)](https://www.dwd.de/DE/leistungen/opendata/help/warnungen/cap_dwd_profile_de_pdf_2_1_13.pdf?__blob=publicationFile&v=3)
    """
    result = await query.alerts(
        ctx['pool'],
        lat=q.lat,
        lon=q.lon,
        warn_cell_id=q.warn_cell_id,
    )
    enhance(result, timezone=q.timezone)
    return ORJSONResponse(result)
