import logging
import os

from brightsky.export import DBExporter
from brightsky.parsers import get_parser
from brightsky.polling import DWDPoller
from brightsky.utils import dwd_fingerprint


logger = logging.getLogger('brightsky')


def parse(path=None, url=None, export=False):
    if not path and not url:
        raise ValueError('Please provide either path or url')
    parser_cls = get_parser(os.path.basename(path or url))
    parser = parser_cls(path=path, url=url)
    if url:
        parser.download()
        fingerprint = {
            'url': url,
            **dwd_fingerprint(parser.path),
        }
    else:
        fingerprint = None
    records = parser.parse()
    if export:
        exporter = DBExporter()
        exporter.export(records, fingerprint=fingerprint)
    return records


def poll(enqueue=False):
    updated_files = DWDPoller().poll()
    if enqueue:
        from brightsky.worker import huey, process
        pending_urls = [
            t.args[0] for t in huey.pending() if t.name == 'process']
        for updated_file in updated_files:
            url = updated_file['url']
            if url in pending_urls:
                logger.debug('Skipping "%s": already queued', url)
                continue
            elif f'brightsky.lock.{url}' in huey._locks:
                logger.debug('Skipping "%s": already running', url)
                continue
            logger.debug('Enqueueing "%s"', url)
            process(url)
    return updated_files
