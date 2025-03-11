Bright Sky is a free and open-source weather API. It aims to provide an easy-to-use gateway to weather data that the [DWD](https://www.dwd.de/) – Germany's meteorological service – publishes on their [open data server](https://opendata.dwd.de/).

The public instance at `https://api.brightsky.dev/` is free-to-use for all purposes, **no API key required**! Please note that the [DWD's Terms of Use](https://www.dwd.de/EN/service/copyright/copyright_artikel.html) apply to all data you retrieve through the API.

> This documentation is generated from an OpenAPI specification. The current version is available from https://api.brightsky.dev/openapi.json.


## Quickstart

* Check out [`/current_weather`](operations/getCurrentWeather) if you want to know what the weather's like _right now_.
* Check out [`/weather`](operations/getWeather) for hourly weather observations and forecasts.
* Check out [`/radar`](operations/getRadar) if you're looking for a high-resolution rain radar.
* Check out [`/alerts`](operations/getAlerts) if you're interested in weather alerts.

... or keep reading below for some background information.


## Good to Know

* **Geographical coverage**: due to its nature as German meteorological service, the observations published by the DWD have a strong focus on Germany. The _forecasts_ cover the whole world, albeit at a much lower density outside of Germany.
* **Historical coverage**: Bright Sky serves historical data going back to January 1st, 2010. If you need data that goes further back, check out our [infrastructure repository](https://github.com/jdemaeyer/brightsky-infrastructure) to easily set up your own instance of Bright Sky!
* **Source IDs**: Bright Sky's _source IDs_ are a technical artifact and – unlike the [DWD station IDs](https://www.dwd.de/DE/leistungen/klimadatendeutschland/stationsliste.html) and [WMO station IDs](https://opendata.dwd.de/climate_environment/CDC/help/stations_list_CLIMAT_data.txt) – have no meaning in the real world. When making requests to Bright Sky, try to avoid them and supply lat/lon or station IDs instead.


## Useful Links

* [Bright Sky source code and issue tracking](https://github.com/jdemaeyer/brightsky/)
* [Bright Sky infrastructure configuration](https://github.com/jdemaeyer/brightsky-infrastructure/)
* [DWD Open Data landing page](https://www.dwd.de/EN/ourservices/opendata/opendata.html)
* [Additional explanation files for DWD Open Data](https://www.dwd.de/DE/leistungen/opendata/hilfe.html?nn=495490&lsbId=627548), including:
    * [List of main observation stations](https://www.dwd.de/DE/leistungen/opendata/help/stationen/ha_messnetz.xls?__blob=publicationFile&v=1)
    * [List of additional observation stations](https://www.dwd.de/DE/leistungen/opendata/help/stationen/na_messnetz.xlsx?__blob=publicationFile&v=10)
    * [List of MOSMIX stations](https://www.dwd.de/DE/leistungen/met_verfahren_mosmix/mosmix_stationskatalog.cfg?view=nasPublication&nn=495490)
    * [List of meteorological parameters](https://www.dwd.de/DE/leistungen/opendata/help/schluessel_datenformate/kml/mosmix_elemente_pdf.pdf?__blob=publicationFile&v=2)
* [DWD Open Data FAQ (German)](https://www.dwd.de/DE/leistungen/opendata/faqs_opendata.html)
* [DWD Copyright information](https://www.dwd.de/EN/service/copyright/copyright_artikel.html)


## Data Sources

All data available through Bright Sky is taken or derived from data on the [DWD open data server](https://opendata.dwd.de/):

* **Current weather / SYNOP**:
  * https://opendata.dwd.de/weather/weather_reports/synoptic/germany/json/
* **Hourly weather**:
  * Historical: https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/
  * Current day: https://opendata.dwd.de/weather/weather_reports/poi/
  * Forecasts: https://opendata.dwd.de/weather/local_forecasts/mos/
* **Radar**:
  * https://opendata.dwd.de/weather/radar/composite/rv/
* **Alerts**:
  * https://opendata.dwd.de/weather/alerts/cap/COMMUNEUNION_DWD_STAT/\
