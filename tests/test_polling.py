import datetime
from unittest.mock import patch

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
        '/dir/stundenwerte_TD_00163_akt.zip': (
            'DewPointObservationsParser', '2020-03-29 09:02', 70167),
        '/dir/stundenwerte_TU_00161_akt.zip': (
            'TemperatureObservationsParser', '2020-03-29 09:00', 70165),
        '/dir/stundenwerte_VV_00164_akt.zip': (
            'VisibilityObservationsParser', '2020-03-29 09:03', 70168),
        '/dir/stundenwerte_N_00162_akt.zip': (
            'CloudCoverObservationsParser', '2020-03-29 09:01', 70166),
        '/dir/MOSMIX_L_LATEST.kmz': (
            'MOSMIXParser', '2020-03-29 09:57', 91636300),
        '/dir/MOSMIX_S_LATEST_240.kmz': (
            'MOSMIXParser', '2020-03-29 10:21', 38067304),
        '/dir/K611_-BEOB.csv': (
            'CurrentObservationsParser', '2020-04-06 10:38', 7343),
        '/dir/10minutenwerte_extrema_wind_01766_now.zip': (
            'WindGustsObservationsParser', '2020-06-08 09:12', 701),
        '/dir/10minutenwerte_extrema_wind_01766_akt.zip': (
            'WindGustsObservationsParser', '2020-06-08 04:27', 519692),
        '/dir/10minutenwerte_extrema_wind_01766_20100101_20191231_hist.zip': (
            'WindGustsObservationsParser', '2020-04-09 09:16', 3661729),
        '/dir/10minutenwerte_SOLAR_01766_akt.zip': (
            'SolarRadiationObservationsParser', '2023-04-12 00:50', 367557),
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


def test_dwdpoller_poll_ignores_parsed_files(db, data_dir):
    poller = DWDPoller()
    poller.urls = ['http://example.com/']
    url = 'http://example.com/stundenwerte_FF_00011_akt.zip'
    with open(data_dir / 'dwd_opendata_index.html') as f:
        resp_text = f.read()
    config = {'get.return_value.text': resp_text}
    with patch('brightsky.polling.requests', **config):
        urls = [info['url'] for info in poller.poll()]
        with db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO parsed_files (
                    url, last_modified, file_size, parsed_at)
                VALUES (%s, %s, %s, current_timestamp)
                """,
                (url, '2020-03-29 08:55', 70523))
        db.commit()
        new_urls = [info['url'] for info in poller.poll()]
        assert url in urls
        assert url not in new_urls
        assert len(new_urls) == len(urls) - 1
