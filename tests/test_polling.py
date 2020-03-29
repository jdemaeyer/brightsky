from brightsky.polling import DWDPoller


def test_dwdpoller_parse(data_dir):
    with open(data_dir / 'dwd_opendata_index.html') as f:
        resp_text = f.read()
    expected_parsers = {
        '/dir/stundenwerte_FF_00011_akt.zip': 'WindObservationsParser',
        '/dir/stundenwerte_FF_00090_akt.zip': 'WindObservationsParser',
        '/dir/stundenwerte_P0_00096_akt.zip': 'PressureObservationsParser',
        '/dir/stundenwerte_RR_00102_akt.zip': (
            'PrecipitationObservationsParser'),
        '/dir/stundenwerte_SD_00125_akt.zip': 'SunshineObservationsParser',
        '/dir/stundenwerte_TU_00161_akt.zip': 'TemperatureObservationsParser',
        '/dir/MOSMIX_S_LATEST_240.kmz': 'MOSMIXParser',
    }
    assert list(DWDPoller().parse('/dir/', resp_text)) == [
        {'url': k, 'parser': v} for k, v in expected_parsers.items()]
