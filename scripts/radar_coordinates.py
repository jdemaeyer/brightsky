#!/usr/bin/env python

import json
import os.path
import re
import subprocess
import tempfile

proj_str = '+proj=stere +lat_0=90 +lat_ts=60 +lon_0=10 +a=6378137 +b=6356752.3142451802 +no_defs +x_0=543196.83521776402 +y_0=3622588.8619310018 -f "%.10g"'  # noqa

HEIGHT = 1200
WIDTH = 1100


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        in_path = os.path.join(tmpdir, 'in')
        with open(in_path, 'w') as f:
            for y in range(HEIGHT):
                for x in range(WIDTH):
                    f.write(f'{x*1000} {-y*1000}\n')
        p = subprocess.run(
            f'invproj {proj_str} {in_path}',
            shell=True,
            cwd=tmpdir,
            capture_output=True,
            text=True,
        )
        coords = []
        for line in p.stdout.splitlines():
            xy = [float(x) for x in re.split(r'\s+', line.strip())]
            assert len(xy) == 2
            coords.append(xy)
        coords = [
            coords[row*WIDTH:(row+1)*WIDTH]
            for row in range(HEIGHT)
        ]
        with open('radar_coordinates.json', 'w') as f:
            # It'd be nicer if we could keep the proj output as strings and
            # convince json to output strings without quotes (so they will be
            # read as float), but that's not easily available through custom
            # encoders...
            json.dump(coords, f)


if __name__ == '__main__':
    main()
