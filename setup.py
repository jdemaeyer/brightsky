from setuptools import setup


setup(
    name='brightsky',
    version='0.1',
    author='Jakob de Maeyer',
    author_email='jakob@naboa.de',
    packages=['brightsky'],
    install_requires=[
        'click',
        'coloredlogs',
        'huey[redis]',
        'parsel',
        'psycopg2-binary',
        'python-dateutil',
        'requests',
    ],
)
