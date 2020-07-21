from brightsky.units import synop_current_weather_code_to_condition


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
