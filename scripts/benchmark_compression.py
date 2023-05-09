import sys
import timeit


URL = 'http://localhost:8000/radar'
BBOXES = [
    None,
    '200,200,800,800',
    '400,400,600,600',
]


def p(s, back=0):
    sys.stdout.write('\b' * back)
    sys.stdout.write(s)
    sys.stdout.flush()


def benchmark():
    for fmt in ['compressed', 'bytes', 'plain']:
        p(f'{fmt:10s}     ')
        for bbox in BBOXES:
            time = timeit.timeit(
                'requests.get(url, params=params).raise_for_status()',
                setup='import requests',
                number=20,
                globals={
                    'url': URL,
                    'params': {
                        'format': fmt,
                        'bbox': bbox,
                    },
                },
            )
            time = int(round(time / 20 * 1000))
            p(f'{time:4d}     ', 3)
        print('')


if __name__ == '__main__':
    benchmark()
