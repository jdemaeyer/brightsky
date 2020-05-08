def celsius_to_kelvin(temperature):
    return round(temperature + 273.15, 2)


def hpa_to_pa(pressure):
    return pressure * 100


def kmh_to_ms(speed):
    return round(speed / 3.6, 1)


def minutes_to_seconds(duration):
    return duration * 60
