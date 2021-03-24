import datetime

from dateutil.tz import tzutc

from brightsky.export import DBExporter, SYNOPExporter
from brightsky.tasks import clean


def test_clean_deletes_expired_parsed_files(db):
    now = datetime.datetime.utcnow().replace(tzinfo=tzutc())
    fingerprints = [
        {
            'url': 'https://example.com/Z__C_EDZW_very_old.json',
            'last_modified': now,
            'file_size': 1234,
            'parsed_at': now - datetime.timedelta(days=14),
        },
        {
            'url': 'https://example.com/Z__C_EDZW_recent.json',
            'last_modified': now,
            'file_size': 1234,
            'parsed_at': now - datetime.timedelta(days=1),
        },
    ]
    db.insert('parsed_files', fingerprints)
    assert len(db.table('parsed_files')) == 2
    clean()
    rows = db.table('parsed_files')
    assert len(rows) == 1
    assert rows[0]['url'].endswith('_recent.json')


PLACE = {
    'lat': 10,
    'lon': 20,
    'height': 30,
    'dwd_station_id': '01766',
    'wmo_station_id': '10315',
    'station_name': 'Münster',
}


def test_clean_deletes_expired_forecast_current_synop_records(db):
    now = datetime.datetime.utcnow().replace(
        minute=0, second=0, microsecond=0, tzinfo=tzutc())
    records = [
        {
            'observation_type': 'forecast',
            'timestamp': now,
            **PLACE,
            'temperature': 10.,
        },
        {
            'observation_type': 'forecast',
            'timestamp': now - datetime.timedelta(hours=6),
            **PLACE,
            'temperature': 20.,
        },
        {
            'observation_type': 'current',
            'timestamp': now,
            **PLACE,
            'temperature': 30.,
        },
        {
            'observation_type': 'current',
            'timestamp': now - datetime.timedelta(hours=6),
            **PLACE,
            'temperature': 40.,
        },
        {
            'observation_type': 'current',
            'timestamp': now - datetime.timedelta(days=3),
            **PLACE,
            'temperature': 50.,
        },
    ]
    synop_records = [
        {
            'observation_type': 'synop',
            'timestamp': now,
            **PLACE,
            'temperature': 60.,
        },
        {
            'observation_type': 'synop',
            'timestamp': now - datetime.timedelta(hours=6),
            **PLACE,
            'temperature': 70.,
        },
        {
            'observation_type': 'synop',
            'timestamp': now - datetime.timedelta(days=3),
            **PLACE,
            'temperature': 80.,
        },
    ]
    DBExporter().export(records)
    SYNOPExporter().export(synop_records)
    assert len(db.table('weather')) == 5
    assert len(db.table('synop')) == 3
    clean()
    assert len(db.table('weather')) == 3
    assert len(db.table('synop')) == 2
    rows = db.fetch('SELECT temperature FROM weather ORDER BY temperature')
    assert [r['temperature'] for r in rows] == [10., 30., 40.]
    rows = db.fetch('SELECT temperature FROM synop ORDER BY temperature')
    assert [r['temperature'] for r in rows] == [60., 70.]
