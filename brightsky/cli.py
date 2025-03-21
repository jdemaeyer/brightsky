import json
import logging

import click
import uvicorn
from fastapi.testclient import TestClient
from huey.consumer_options import ConsumerConfig

from brightsky import db, tasks
from brightsky.utils import parse_date
from brightsky.web import app
from brightsky.worker import huey


def dump_records(it):
    for record in it:
        print(json.dumps(record, default=str))


def migrate_callback(ctx, param, value):
    if value:
        db.migrate()


def parse_date_arg(ctx, param, value):
    if not value:
        return
    return parse_date(value)


@click.group()
@click.option(
    '--migrate', help='Migrate database before running command.',
    is_flag=True, is_eager=True, expose_value=False, callback=migrate_callback)
def cli():
    pass


@cli.command()
def migrate():
    """Apply all pending database migrations."""
    db.migrate()


@cli.command()
@click.argument(
    'targets',
    required=True,
    nargs=-1,
    metavar='TARGET [TARGET ...]',
)
def parse(targets):
    for target in targets:
        tasks.parse(target)


@cli.command()
@click.option(
    '--enqueue/--no-enqueue', default=False,
    help='Enqueue updated files for processing by the worker')
def poll(enqueue):
    """Detect updated files on DWD Open Data Server."""
    files = tasks.poll(enqueue=enqueue)
    if not enqueue:
        dump_records(files)


@cli.command()
def clean():
    """Clean expired forecast and observations from database."""
    tasks.clean()


@cli.command()
@click.option('--workers', default=3, type=int, help='Number of threads')
def work(workers):
    """Start brightsky worker."""
    huey.flush()
    config = ConsumerConfig(worker_type='thread', workers=workers)
    config.validate()
    consumer = huey.create_consumer(**config.values)
    consumer.run()


@cli.command()
@click.option('--bind', default='127.0.0.1:5000', help='Bind address')
@click.option(
    '--reload/--no-reload', default=False,
    help='Reload server on source code changes')
def serve(bind, reload):
    """Start brightsky API webserver."""
    host, port = bind.rsplit(':', 1)
    uvicorn.run(
        'brightsky.web:app',
        host=host,
        port=int(port),
        reload=reload,
    )


@cli.command(context_settings={'ignore_unknown_options': True})
@click.argument('endpoint')
@click.argument('parameters', nargs=-1, type=click.UNPROCESSED)
def query(endpoint, parameters):
    """Query API and print JSON response.

    Parameters must be supplied as --name value or --name=value. See
    https://brightsky.dev/docs/ for the available endpoints and arguments.

    \b
    Examples:
    python -m brightsky query weather --lat 52 --lon 7.6 --date 2018-08-13
    python -m brightsky query current_weather --lat=52 --lon=7.6
    """
    for route in app.routes:
        if route.path == f'/{endpoint}':
            break
    else:
        raise click.UsageError(f"Unknown endpoint '{endpoint}'")
    logging.getLogger().setLevel(logging.WARNING)
    with TestClient(app) as client:
        resp = client.get(f'/{endpoint}', params=_parse_params(parameters))
    print(json.dumps(resp.json()))


def _parse_params(parameters):
    # I'm sure there's a function in click or argparse somewhere that does this
    # but I can't find it
    usage = "Supply API parameters as --name value or --name=value"
    params = {}
    param_name = None
    for param in parameters:
        if param_name is None:
            if not param.startswith('--'):
                raise click.UsageError(usage)
            param = param[2:]
            if '=' in param:
                name, value = param.split('=', 1)
                params[name] = value
            else:
                param_name = param
        else:
            params[param_name] = param
            param_name = None
    if param_name is not None:
        raise click.UsageError(usage)
    return params
