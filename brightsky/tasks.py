import logging
import os

from brightsky.db import get_connection
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
        if (expired_locks := huey.expire_locks(1800)):
            logger.warning(
                'Removed expired locks: %s', ', '.join(expired_locks))
        pending_urls = [
            t.args[0] for t in huey.pending() if t.name == 'process']
        enqueued = 0
        for updated_file in updated_files:
            url = updated_file['url']
            if url in pending_urls:
                logger.debug('Skipping "%s": already queued', url)
                continue
            elif huey.is_locked(url):
                logger.debug('Skipping "%s": already running', url)
                continue
            logger.debug('Enqueueing "%s"', url)
            process(url)
            enqueued += 1
        logger.info('Enqueued %d updated files for processing', enqueued)
    return updated_files


def clean():
    expiry_intervals = {
        'forecast': '12 hours',
        'current': '48 hours',
    }
    logger.info('Deleting expired weather records: %s', expiry_intervals)
    with get_connection() as conn:
        with conn.cursor() as cur:
            for observation_type, expiry_interval in expiry_intervals.items():
                cur.execute(
                    """
                    DELETE FROM weather WHERE
                        source_id IN (
                            SELECT id FROM sources
                            WHERE observation_type = %s) AND
                        timestamp < current_timestamp - %s::interval;
                    """,
                    (observation_type, expiry_interval),
                )
                conn.commit()
                if cur.rowcount:
                    logger.info(
                        'Deleted %d outdated %s weather records',
                        cur.rowcount, observation_type)
