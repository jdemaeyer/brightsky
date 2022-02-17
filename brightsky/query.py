import datetime

from dateutil.tz import tzutc

from brightsky.db import fetch


def _make_dicts(rows):
    return [dict(row) for row in rows]


def weather(
        date, last_date=None, lat=None, lon=None, dwd_station_id=None,
        wmo_station_id=None, source_id=None, max_dist=50000):
    if not last_date:
        last_date = date + datetime.timedelta(days=1)
    if not date.tzinfo:
        date = date.replace(tzinfo=tzutc())
    if not last_date.tzinfo:
        last_date = last_date.replace(tzinfo=tzutc())
    sources_rows = sources(
        lat=lat, lon=lon, dwd_station_id=dwd_station_id, source_id=source_id,
        wmo_station_id=wmo_station_id, max_dist=max_dist,
        observation_types=['historical', 'current', 'forecast'],
        date=date, last_date=last_date)['sources']
    primary_source_ids = {}
    for row in sources_rows:
        primary_source_ids.setdefault(row['observation_type'], row['id'])
    primary_source_ids = list(primary_source_ids.values())
    weather_rows = _weather(date, last_date, primary_source_ids)
    source_ids = [row['id'] for row in sources_rows]
    if len(weather_rows) < int((last_date - date).total_seconds()) // 3600:
        weather_rows = _weather(date, last_date, source_ids)
    _fill_missing_fields(weather_rows, date, last_date, source_ids)
    used_source_ids = {row['source_id'] for row in weather_rows}
    used_source_ids.update(
        source_id
        for row in weather_rows
        for source_id in row.get('fallback_source_ids', {}).values())
    return {
        'weather': weather_rows,
        'sources': [s for s in sources_rows if s['id'] in used_source_ids],
    }


def _weather(date, last_date, source_id, not_null=None):
    params = {
        'date': date,
        'last_date': last_date,
        'source_id': source_id,
    }
    where = "timestamp BETWEEN %(date)s AND %(last_date)s"
    order_by = "timestamp"
    if isinstance(source_id, list):
        where += " AND source_id IN %(source_id_tuple)s"
        order_by += ", array_position(%(source_id)s, source_id)"
        params['source_id_tuple'] = tuple(source_id)
    else:
        where += " AND source_id = %(source_id)s"
    if not_null:
        where += ''.join(f" AND {element} IS NOT NULL" for element in not_null)
    sql = f"""
        SELECT DISTINCT ON (timestamp) *
        FROM weather
        WHERE {where}
        ORDER BY {order_by}
    """
    return _make_dicts(fetch(sql, params))


IGNORED_MISSING_FIELDS = {
    # Not available in MOSMIX
    'wind_gust_direction', 'relative_humidity',
    # Missing for many stations during nighttime
    'sunshine',
}


def _fill_missing_fields(weather_rows, date, last_date, source_ids):
    incomplete_rows = []
    missing_fields = set()
    for row in weather_rows:
        missing_row_fields = set(
            k for k, v in row.items() if v is None
        ).difference(IGNORED_MISSING_FIELDS)
        if missing_row_fields:
            incomplete_rows.append((row, missing_row_fields))
            missing_fields.update(missing_row_fields)
    if incomplete_rows:
        min_date = incomplete_rows[0][0]['timestamp']
        max_date = incomplete_rows[-1][0]['timestamp']
        # NOTE: If there are multiple missing fields we may be missing out on
        #       a "better" fallback if there are preferred sources that have
        #       one (but not all) of the missing fields. However, this lets us
        #       get by with using the basic weather query, and with performing
        #       it only one extra time.
        fallback_rows = {
            row['timestamp']: row
            for row in _weather(
                min_date, max_date, source_ids, not_null=missing_fields)
        }
        for row, fields in incomplete_rows:
            fallback_row = fallback_rows.get(row['timestamp'])
            if fallback_row:
                row['fallback_source_ids'] = {}
                for f in fields:
                    row[f] = fallback_row[f]
                    row['fallback_source_ids'][f] = fallback_row['source_id']


def current_weather(
        lat=None, lon=None, dwd_station_id=None, wmo_station_id=None,
        source_id=None, max_dist=50000, fallback=True):
    sources_rows = sources(
        lat=lat, lon=lon, dwd_station_id=dwd_station_id,
        wmo_station_id=wmo_station_id, observation_types=['synop'],
        max_dist=max_dist
    )['sources']
    source_ids = [row['id'] for row in sources_rows]
    weather = _current_weather(source_ids)
    if not weather:
        raise LookupError(
            "Could not find current weather for your location criteria")
    used_source_ids = [weather['source_id']]
    if fallback:
        missing_fields = [k for k, v in weather.items() if v is None]
        fallback_weather = _current_weather(
            source_ids, not_null=missing_fields)
        if fallback_weather:
            weather.update({k: fallback_weather[k] for k in missing_fields})
            weather['fallback_source_ids'] = {
                field: fallback_weather['source_id']
                for field in missing_fields}
            used_source_ids.append(fallback_weather['source_id'])
    return {
        'weather': weather,
        'sources': [s for s in sources_rows if s['id'] in used_source_ids],
    }


def _current_weather(source_ids, not_null=None):
    params = {
        'source_ids': source_ids,
        'source_ids_tuple': tuple(source_ids),
    }
    where = "source_id IN %(source_ids_tuple)s"
    if not_null:
        where += ''.join(f" AND {element} IS NOT NULL" for element in not_null)
    sql = f"""
        SELECT *
        FROM current_weather
        WHERE {where}
        ORDER BY array_position(%(source_ids)s, source_id)
        LIMIT 1
    """
    rows = _make_dicts(fetch(sql, params))
    if not rows:
        return {}
    return rows[0]


def synop(
        date, last_date=None, dwd_station_id=None, wmo_station_id=None,
        source_id=None):
    if not last_date:
        last_date = date + datetime.timedelta(days=1)
    sources_rows = sources(
        dwd_station_id=dwd_station_id, wmo_station_id=wmo_station_id,
        source_id=source_id, observation_types=['synop'])['sources']
    source_ids = [row['id'] for row in sources_rows]
    sql = """
        SELECT *
        FROM synop
        WHERE
            timestamp BETWEEN %(date)s AND %(last_date)s AND
            source_id IN %(source_ids_tuple)s
        ORDER BY timestamp
        """
    params = {
        'date': date,
        'last_date': last_date,
        'source_ids_tuple': tuple(source_ids),
    }
    return {
        'weather': _make_dicts(fetch(sql, params)),
        'sources': _make_dicts(sources_rows),
    }


def sources(
        lat=None, lon=None, dwd_station_id=None, wmo_station_id=None,
        source_id=None, observation_types=None, max_dist=50000,
        ignore_type=False, date=None, last_date=None):
    select = "*"
    order_by = "observation_type"
    params = {
        'lat': lat,
        'lon': lon,
        'max_dist': max_dist,
        'dwd_station_id': dwd_station_id,
        'wmo_station_id': wmo_station_id,
        'source_id': source_id,
        'observation_types': tuple(observation_types or ()),
        'date': date,
        'last_date': last_date,
    }
    if source_id is not None:
        if isinstance(source_id, list):
            where = "id IN %(source_id_tuple)s"
            order_by = (
                "array_position(%(source_id)s, id), observation_type")
            params['source_id_tuple'] = tuple(source_id)
        else:
            where = "id = %(source_id)s"
    elif dwd_station_id is not None:
        if isinstance(dwd_station_id, list):
            where = "dwd_station_id IN %(dwd_station_id_tuple)s"
            order_by = (
                "array_position(%(dwd_station_id)s, dwd_station_id::text), "
                "observation_type")
            params['dwd_station_id_tuple'] = tuple(dwd_station_id)
        else:
            where = "dwd_station_id = %(dwd_station_id)s"
    elif wmo_station_id is not None:
        if isinstance(wmo_station_id, list):
            where = "wmo_station_id IN %(wmo_station_id_tuple)s"
            order_by = (
                "array_position(%(wmo_station_id)s, wmo_station_id::text), "
                "observation_type")
            params['wmo_station_id_tuple'] = tuple(wmo_station_id)
        else:
            where = "wmo_station_id = %(wmo_station_id)s"
    elif (lat is not None and lon is not None):
        distance = """
            earth_distance(
                ll_to_earth(%(lat)s, %(lon)s),
                ll_to_earth(lat, lon)
            )
        """
        select += f", round({distance}) AS distance"
        where = f"""
            earth_box(
                ll_to_earth(%(lat)s, %(lon)s),
                %(max_dist)s
            ) @> ll_to_earth(lat, lon) AND
            {distance} < %(max_dist)s
        """
        if ignore_type:
            order_by = "distance"
        else:
            order_by += ", distance"
    else:
        raise ValueError(
            "Please supply lat/lon or dwd_station_id or wmo_station_id or "
            "source_id")
    if observation_types:
        where += " AND observation_type IN %(observation_types)s"
    if date is not None:
        where += " AND last_record >= %(date)s"
    if last_date is not None:
        where += " AND first_record <= %(last_date)s"
    sql = f"""
        SELECT {select}
        FROM sources
        WHERE {where}
        ORDER BY {order_by}
        """
    rows = fetch(sql, params)
    if not rows:
        raise LookupError("No sources match your criteria")
    return {'sources': _make_dicts(rows)}
