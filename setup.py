from setuptools import setup

import brightsky


with open('README.md') as f:
    long_description = f.read()

setup(
    name='brightsky',
    version=brightsky.__version__,
    author='Jakob de Maeyer',
    author_email='jakob@naboa.de',
    description="JSON API for DWD's open weather data.",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://brightsky.dev/',
    project_urls={
        'Documentation': 'https://brightsky.dev/docs/',
        'Source': 'https://github.com/jdemaeyer/brightsky/',
        'Tracker': 'https://github.com/jdemaeyer/brightsky/issues/',
    },
    packages=['brightsky'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    install_requires=[
        'astral',
        'click',
        'coloredlogs',
        'falcon==2.*',
        'falcon-cors',
        'gunicorn',
        'huey',
        'parsel',
        'psycopg2-binary<2.9',
        'python-dateutil',
        # huey is incompatible with redis4
        'redis<4',
        'requests',
        'sentry-sdk',
    ],
)
