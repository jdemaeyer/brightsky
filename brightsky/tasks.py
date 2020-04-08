import logging
import os

from brightsky import parsers
from brightsky.export import DBExporter
from brightsky.polling import DWDPoller


logger = logging.getLogger('brightsky')


def parse(path=None, url=None, export=False):
    if not path and not url:
        raise ValueError('Please provide either path or url')
    parser_cls = parsers.get_parser(os.path.basename(path or url))
    parser = parser_cls(path=path, url=url)
    if url:
        parser.download()
    records = parser.parse()
    if export:
        exporter = DBExporter()
        exporter.export(records)
    return records


def poll():
    return DWDPoller().poll()
