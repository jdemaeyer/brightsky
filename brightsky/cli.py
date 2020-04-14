import json
from multiprocessing import cpu_count

import click
from huey.consumer_options import ConsumerConfig

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
@click.option('--enqueue/--no-enqueue', default=False)
def poll(enqueue):
    files = tasks.poll(enqueue=enqueue)
    if not enqueue:
        dump_records(files)


@cli.command(help='Clean expired forecast and observations from database')
def clean():
    tasks.clean()


@cli.command(help='Start brightsky worker')
def work():
    from brightsky.worker import huey
    huey.flush()
    config = ConsumerConfig(worker_type='thread', workers=2*cpu_count()+1)
    config.validate()
    consumer = huey.create_consumer(**config.values)
    consumer.run()
