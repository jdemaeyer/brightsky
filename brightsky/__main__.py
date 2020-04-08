from brightsky.cli import cli
from brightsky.utils import configure_logging, load_dotenv


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    cli(prog_name='python -m brightsky')
