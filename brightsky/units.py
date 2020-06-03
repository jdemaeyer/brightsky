def celsius_to_kelvin(temperature):
    return round(temperature + 273.15, 2)


def hpa_to_pa(pressure):
    return int(pressure * 100)


def kelvin_to_celsius(temperature):
    return round(temperature - 273.15, 2)


def kmh_to_ms(speed):
    return round(speed / 3.6, 1)


def minutes_to_seconds(duration):
    return int(duration * 60)


def ms_to_kmh(speed):
    return round(speed * 3.6, 1)


def pa_to_hpa(pressure):
    return pressure / 100


def seconds_to_minutes(duration):
    return duration / 60


CONVERTERS = {
    'dwd': {
        'pressure_msl': pa_to_hpa,
        'sunshine': seconds_to_minutes,
        'temperature': kelvin_to_celsius,
        'wind_speed': ms_to_kmh,
    }
}


def convert_record(record, units):
    for field, converter in CONVERTERS[units].items():
        if record.get(field) is not None:
            record[field] = converter(record[field])
