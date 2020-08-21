from brightsky.units import (
    convert_record, synop_current_weather_code_to_condition)


def test_synop_current_weather_code_to_condition():
    expected = {
        -1: None,
        0: 'dry',
        10: 'dry',
        11: 'dry',
        17: 'thunderstorm',
        18: 'dry',
        196: 'hail',
        197: None,
        198: None,
        199: 'thunderstorm',
        500: None,

        0.0: 'dry',
        16.0: 'dry',
        17.0: 'thunderstorm',
        18.0: 'dry',
    }
    for code, exp_condition in expected.items():
        assert synop_current_weather_code_to_condition(code) == exp_condition


def test_convert_record():
    record = {
        'source_id': 11695,
        'timestamp': '2020-08-18T13:00:00+00:00',
        'dew_point': 285.96,
        'precipitation_60': 0,
        'pressure_msl': 101050,
        'relative_humidity': 51,
        'visibility': 75000,
        'wind_direction_60': 248,
        'wind_speed_60': 5,
        'wind_gust_direction_60': 230,
        'wind_gust_speed_60': 9.1,
        'sunshine_60': 1980,
        'temperature': 296.65,
        'icon': 'cloudy',
    }
    expected = {
        'source_id': 11695,
        'timestamp': '2020-08-18T13:00:00+00:00',
        'dew_point': 12.81,
        'precipitation_60': 0,
        'pressure_msl': 1010.5,
        'relative_humidity': 51,
        'visibility': 75000,
        'wind_direction_60': 248,
        'wind_speed_60': 18,
        'wind_gust_direction_60': 230,
        'wind_gust_speed_60': 32.8,
        'sunshine_60': 33,
        'temperature': 23.5,
        'icon': 'cloudy',
    }
    convert_record(record, 'dwd')
    assert record == expected
