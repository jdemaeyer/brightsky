import datetime

from dateutil.tz import tzutc

from brightsky.polling import DWDPoller


def test_dwdpoller_parse(data_dir):
    with open(data_dir / 'dwd_opendata_index.html') as f:
        resp_text = f.read()
    expected = {
        '/dir/stundenwerte_FF_00011_akt.zip': (
            'WindObservationsParser', '2020-03-29 08:55', 70523),
        '/dir/stundenwerte_FF_00090_akt.zip': (
            'WindObservationsParser', '2020-03-29 08:56', 71408),
        '/dir/stundenwerte_P0_00096_akt.zip': (
            'PressureObservationsParser', '2020-03-29 08:57', 47355),
        '/dir/stundenwerte_RR_00102_akt.zip': (
            'PrecipitationObservationsParser', '2020-03-29 08:58', 74372),
        '/dir/stundenwerte_SD_00125_akt.zip': (
            'SunshineObservationsParser', '2020-03-29 08:59', 69633),
        '/dir/stundenwerte_TU_00161_akt.zip': (
            'TemperatureObservationsParser', '2020-03-29 09:00', 70165),
        '/dir/MOSMIX_S_LATEST_240.kmz': (
            'MOSMIXParser', '2020-03-29 10:21', 38067304),
        '/dir/K611_-BEOB.csv': (
            'CurrentObservationsParser', '2020-04-06 10:38', 7343),
    }
    assert list(DWDPoller().parse('/dir/', resp_text)) == [
        {
            'url': k,
            'parser': v[0],
            'last_modified': datetime.datetime.strptime(
                v[1], '%Y-%m-%d %H:%M').replace(tzinfo=tzutc()),
            'file_size': v[2],
        }
        for k, v in expected.items()]
