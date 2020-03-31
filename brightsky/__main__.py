import json
import logging
import os
from contextlib import suppress

import click
import coloredlogs

from brightsky import db, parsers
from brightsky.export import DBExporter
from brightsky.polling import DWDPoller


logger = logging.getLogger('brightsky')


def configure_logging():
    log_fmt = '%(asctime)s %(name)s %(levelname)s  %(message)s'
    coloredlogs.install(level=logging.DEBUG, fmt=log_fmt)
    # Disable some third-party noise
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def load_dotenv(path='.env'):
    with suppress(FileNotFoundError):
        with open(path) as f:
            for line in f:
                if line.strip() and not line.strip().startswith('#'):
                    key, val = line.strip().split('=', 1)
                    os.environ.setdefault(key, val)


def dump_records(it):
    for record in it:
        print(json.dumps(record, default=str))


@click.group()
def cli():
    pass


@cli.command(help='Apply all pending database migrations')
def migrate():
    db.migrate()


@cli.command(help='Parse a forecast or observations file')
@click.option('--path')
@click.option('--url')
@click.option('--export/--no-export', default=False)
def parse(path, url, export):
    if not path and not url:
        raise click.ClickException('Please provide either --path or --url')
    parser_name = DWDPoller().get_parser(os.path.basename(path or url))
    parser = getattr(parsers, parser_name)(path=path, url=url)
    if url:
        parser.download()
    logger.info("Parsing %s with %s", path or url, parser_name)
    records = parser.parse()
    if export:
        exporter = DBExporter()
        exporter.export(records)
    else:
        dump_records()


@cli.command(help='Detect updated files on DWD Open Data Server')
def poll():
    logger.info("Polling DWD Open Data Server for updated files")
    dump_records(DWDPoller().poll())


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    cli(prog_name='python -m brightsky')
