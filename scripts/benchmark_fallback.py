#!/usr/bin/env python

import statistics
import sys
import time

import click
import requests


SERVER = 'http://localhost:5000'

IGNORED_FALLBACK_FIELDS = {
    'relative_humidity',
    'wind_gust_direction',
    'precipitation_probability',
    'precipitation_probability_6h',
}

SCENARIOS = {
    'a) No fallback': {
        'endpoint': '/weather',
        'params': {
            'lat': 52.3813,
            'lon': 13.0622,
            'date': '2026-04-20',
        },
        'description': 'Potsdam — full station, no missing fields',
    },
    'b) One-field fallback': {
        'endpoint': '/weather',
        'params': {
            'lat': 51.3,
            'lon': 9.5,
            'date': '2026-04-20',
        },
        'description': 'Schauenburg — sunshine filled from Göttingen',
    },
    'c) Multi-field, single source': {
        'endpoint': '/weather',
        'params': {
            'lat': 52.45,
            'lon': 13.3,
            'date': '2026-04-20',
        },
        'description': 'Berlin-Dahlem — wind fields filled from Tempelhof',
    },
    'd) Multi-field, multi source': {
        'endpoint': '/weather',
        'params': {
            'lat': 52.50,
            'lon': 13.40,
            'date': '2026-04-20',
        },
        'description': 'Berlin-Kreuzberg — most Tempelhof, sunshine Potsdam',
    },
    'e) Current, no fallback': {
        'endpoint': '/current_weather',
        'params': {
            'lat': 52.3813,
            'lon': 13.0622,
        },
        'description': 'Potsdam — full synop station',
    },
    'f) Current, with fallback': {
        'endpoint': '/current_weather',
        'params': {
            'lat': 52.45,
            'lon': 13.3,
        },
        'description': 'Berlin-Dahlem — many fields from Potsdam',
    },
    'g) Stuttgart, full w/ solar': {
        'endpoint': '/weather',
        'params': {
            'lat': 48.78,
            'lon': 9.18,
            'date': '2026-04-20',
        },
        'description': 'Stuttgart — full station, has solar natively',
    },
    'h) München, solar-only gap': {
        'endpoint': '/weather',
        'params': {
            'lat': 48.14,
            'lon': 11.58,
            'date': '2026-04-20',
        },
        'description': 'München-Stadt — solar is only missing field',
    },
    'i) Frankfurt, solar via fallback': {
        'endpoint': '/weather',
        'params': {
            'lat': 50.1,
            'lon': 8.68,
            'date': '2026-04-20',
        },
        'description': 'Frankfurt — solar from Frankfurt/Main via fallback',
    },
    'j) Lübeck, rural fallback': {
        'endpoint': '/weather',
        'params': {
            'lat': 53.87,
            'lon': 10.69,
            'date': '2026-04-20',
        },
        'description': 'Lübeck — solar+sunshine from Boltenhagen',
    },
    'k) Dresden, urban fallback': {
        'endpoint': '/weather',
        'params': {
            'lat': 51.05,
            'lon': 13.74,
            'date': '2026-04-20',
        },
        'description': 'Dresden — solar from Klotzsche',
    },
    'l) Duisburg, urban fallback': {
        'endpoint': '/weather',
        'params': {
            'lat': 51.48,
            'lon': 6.76,
            'date': '2026-04-20',
        },
        'description': 'Duisburg — solar from Essen',
    },
}


def analyze_fallback(data):
    weather = data.get('weather', [])
    if isinstance(weather, dict):
        weather = [weather]
    sources = {
        s['id']: s.get('station_name', s['id'])
        for s in data.get('sources', [])
    }
    total = len(weather)
    rows_with_fallback = 0
    fallback_fields = set()
    fallback_sources = set()
    rows_with_nulls = 0
    null_fields = set()

    for row in weather:
        fb = row.get('fallback_source_ids', {})
        relevant_fb = {
            k: v
            for k, v in fb.items()
            if k not in IGNORED_FALLBACK_FIELDS
        }
        if relevant_fb:
            rows_with_fallback += 1
            fallback_fields.update(relevant_fb.keys())
            fallback_sources.update(relevant_fb.values())
        row_nulls = {
            k for k, v in row.items()
            if v is None and k not in IGNORED_FALLBACK_FIELDS
            and k not in ('icon', 'condition', 'fallback_source_ids')
        }
        if row_nulls:
            rows_with_nulls += 1
            null_fields.update(row_nulls)

    fb_source_names = [
        f"{sid} ({sources.get(sid, '?')})"
        for sid in sorted(fallback_sources)
    ]
    return {
        'total_rows': total,
        'rows_with_fallback': rows_with_fallback,
        'fallback_fields': sorted(fallback_fields),
        'fallback_sources': fb_source_names,
        'rows_with_nulls': rows_with_nulls,
        'null_fields': sorted(null_fields),
    }


def run_scenario(name, scenario, n, warmup):
    endpoint = scenario['endpoint']
    params = scenario['params']
    url = f"{SERVER}{endpoint}"

    for _ in range(warmup):
        requests.get(url, params=params)

    timings = []
    last_data = None
    for _ in range(n):
        start = time.perf_counter()
        resp = requests.get(url, params=params)
        elapsed = time.perf_counter() - start
        resp.raise_for_status()
        timings.append(elapsed * 1000)
        last_data = resp.json()

    analysis = analyze_fallback(last_data)

    click.echo(f"\n{'=' * 60}")
    click.echo(f"  {name}")
    click.echo(f"  {scenario['description']}")
    click.echo(f"{'=' * 60}")
    click.echo(f"  Params:  {params}")
    click.echo(f"  Records: {analysis['total_rows']}")
    if analysis['rows_with_fallback']:
        click.echo(
            f"  Fallback rows: "
            f"{analysis['rows_with_fallback']}/{analysis['total_rows']}",
        )
        click.echo(
            f"  Fallback fields: {', '.join(analysis['fallback_fields'])}",
        )
        click.echo(
            f"  Fallback sources: {', '.join(analysis['fallback_sources'])}",
        )
    else:
        click.echo("  Fallback rows: 0")
    if analysis['rows_with_nulls']:
        click.echo(
            f"  Rows still missing data: "
            f"{analysis['rows_with_nulls']}/{analysis['total_rows']}",
        )
        click.echo(
            f"  Still-null fields: {', '.join(analysis['null_fields'])}",
        )

    click.echo(f"\n  Latency ({n} requests, {warmup} warmup):")
    click.echo(f"    mean: {statistics.mean(timings):8.1f} ms")
    click.echo(f"     p50: {statistics.median(timings):8.1f} ms")
    p95 = sorted(timings)[int(len(timings) * 0.95)]
    click.echo(f"     p95: {p95:8.1f} ms")
    click.echo(f"     min: {min(timings):8.1f} ms")
    click.echo(f"     max: {max(timings):8.1f} ms")

    return timings


@click.command()
@click.option('-n', default=50, help='Number of requests per scenario')
@click.option('--warmup', default=3, help='Warmup requests per scenario')
@click.option(
    '--server',
    default=SERVER,
    help='Base URL of the brightsky server',
)
@click.option(
    '--scenario',
    '-s',
    multiple=True,
    help='Run only specific scenarios (a, b, c, d)',
)
def main(n, warmup, server, scenario):
    """Benchmark fallback behavior in /weather queries."""
    global SERVER
    SERVER = server

    click.echo(f"Benchmarking against {SERVER}")
    click.echo(f"  {n} requests per scenario, {warmup} warmup requests\n")

    try:
        requests.get(f"{SERVER}/", timeout=10)
    except requests.ConnectionError:
        click.echo(f"Error: cannot connect to {SERVER}", err=True)
        sys.exit(1)

    filter_keys = set()
    if scenario:
        for s in scenario:
            for key in SCENARIOS:
                if key.startswith(s):
                    filter_keys.add(key)

    all_timings = {}
    for name, sc in SCENARIOS.items():
        if filter_keys and name not in filter_keys:
            continue
        all_timings[name] = run_scenario(name, sc, n, warmup)

    click.echo(f"\n{'=' * 60}")
    click.echo("  Summary (p50 ms)")
    click.echo(f"{'=' * 60}")
    for name, timings in all_timings.items():
        label = name.split(')')[0] + ')'
        click.echo(f"  {label:36s} {statistics.median(timings):8.1f} ms")


if __name__ == '__main__':
    main()
