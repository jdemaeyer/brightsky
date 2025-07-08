import math

from dwdparse.units import convert_record, kelvin_to_celsius

from brightsky.settings import settings
from brightsky.utils import daytime


def enhance(result, timezone=None, units='si'):
    if 'sources' in result:
        enhance_sources(result['sources'], timezone=timezone)
    if 'weather' in result:
        source_map = {s['id']: s for s in result['sources']}
        enhance_records(
            result['weather'],
            source_map,
            timezone=timezone,
            units=units,
        )
    if 'radar' in result:
        enhance_radar(result['radar'], timezone=timezone)
    if 'alerts' in result:
        enhance_alerts(result['alerts'], timezone=timezone)


def enhance_records(records, source_map, timezone=None, units='si'):
    if not isinstance(records, list):
        records = [records]
    for record in records:
        if record["relative_humidity"] is None:
            record["relative_humidity"] = get_relative_humidity(record, units)
        record['icon'] = get_icon(record, source_map)
        if units != 'si':
            convert_record(record, units)
        process_timestamp(record, 'timestamp', timezone)


def enhance_sources(sources, timezone=None):
    for source in sources:
        process_timestamp(source, 'first_record', timezone)
        process_timestamp(source, 'last_record', timezone)


def enhance_radar(radar, timezone=None):
    for record in radar:
        process_timestamp(record, 'timestamp', timezone)


def enhance_alerts(alerts, timezone=None):
    for alert in alerts:
        for key in ['effective', 'onset', 'expires']:
            process_timestamp(alert, key, timezone)


def process_timestamp(o, key, timezone):
    if not timezone:
        return
    elif not o[key]:
        return
    o[key] = o[key].astimezone(timezone)


def get_icon(record, source_map):
    if record['condition'] in (
            'fog', 'sleet', 'snow', 'hail', 'thunderstorm'):
        return record['condition']
    try:
        precipitation = record['precipitation']
    except KeyError:
        precipitation = record['precipitation_10']
    try:
        wind_speed = record['wind_speed']
    except KeyError:
        wind_speed = record['wind_speed_10']
    # Don't show 'rain' icon for little precipitation, and do show 'rain'
    # icon when condition is None but there is significant precipitation
    is_rainy = (
        record['condition'] == 'rain' and precipitation is None) or (
        (precipitation or 0) > settings.ICON_RAIN_THRESHOLD)
    if is_rainy:
        return 'rain'
    elif (wind_speed or 0) > settings.ICON_WIND_THRESHOLD:
        return 'wind'
    elif (record['cloud_cover'] or 0) >= settings.ICON_CLOUDY_THRESHOLD:
        return 'cloudy'
    source = source_map[record['source_id']]
    daytime_str = daytime(source['lat'], source['lon'], record['timestamp'])
    if (record['cloud_cover'] or 0) >= settings.ICON_PARTLY_CLOUDY_THRESHOLD:
        return f'partly-cloudy-{daytime_str}'
    return f'clear-{daytime_str}'


def get_relative_humidity(record, units):
    """
    Calculate relative humidity from dewpoint and temperature based on the August-Roche-Magnus formula.
    """
    if record["temperature"] is None or record["dew_point"] is None:
        return None
    
    temperature = kelvin_to_celsius(record["temperature"])
    dew_point = kelvin_to_celsius(record["dew_point"])
    if temperature >= 0.0:
        a, b = 17.62, 243.12
    else:
        a, b = 22.46, 272.62
    exponent = (a * dew_point) / (b + dew_point) - (a * temperature) / (b + temperature)
    rh = 100.0 * math.exp(exponent)
    rh = max(0.0, min(rh, 100.0))

    return math.ceil(rh)
