import logging
import os

from brightsky import parsers
from brightsky.export import DBExporter
from brightsky.polling import DWDPoller


logger = logging.getLogger('brightsky')


def parse(path=None, url=None, export=False):
    if not path and not url:
        raise ValueError('Please provide either path or url')
    parser_name = DWDPoller().get_parser(os.path.basename(path or url))
    parser = getattr(parsers, parser_name)(path=path, url=url)
    if url:
        parser.download()
    logger.info("Parsing %s with %s", path or url, parser_name)
    records = parser.parse()
    if export:
        exporter = DBExporter()
        exporter.export(records)
    return records


def poll():
    logger.info("Polling DWD Open Data Server for updated files")
    return DWDPoller().poll()
