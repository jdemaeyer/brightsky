import os

from brightsky.cli import cli
from brightsky.utils import configure_logging, load_dotenv


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    if os.getenv('SENTRY_DSN'):
        import sentry_sdk
        sample_rate = os.getenv('SENTRY_TRACES_SAMPLE_RATE')
        if sample_rate:
            sample_rate = float(sample_rate)
        sentry_sdk.init(
            dsn=os.getenv('SENTRY_DSN'),
            traces_sample_rate=sample_rate)
    cli(prog_name='python -m brightsky')
