import argparse
import json
import logging

import coloredlogs

from brightsky.parsers import MOSMIXParser


logger = logging.getLogger('brightsky')


parser = argparse.ArgumentParser('brightsky')
parser.add_argument('--mosmix-path', help='Path to MOSMIX kmz file')


def configure_logging():
    log_fmt = '%(asctime)s %(name)s %(levelname)s  %(message)s'
    coloredlogs.install(level=logging.DEBUG, fmt=log_fmt)
    # Disable some third-party noise
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def main(mosmix_path):
    p = MOSMIXParser(path=mosmix_path)
    if mosmix_path is None:
        logger.info('Downloading MOSMIX data from DWD')
        p.download()
    logger.info('Parsing MOSMIX records')
    for record in p.parse():
        print(json.dumps(record, default=str))


if __name__ == '__main__':
    configure_logging()
    args = parser.parse_args()
    main(args.mosmix_path)
