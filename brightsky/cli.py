import json

import click

from brightsky import db, tasks


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
    records = tasks.parse(path=path, url=url, export=export)
    if not export:
        dump_records(records)


@cli.command(help='Detect updated files on DWD Open Data Server')
def poll():
    dump_records(tasks.poll())
