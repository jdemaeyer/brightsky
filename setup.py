from setuptools import setup


setup(
    name='brightsky',
    version='0.1a',
    author='Jakob de Maeyer',
    author_email='jakob@naboa.de',
    packages=['brightsky'],
    install_requires=[
        'coloredlogs',
        'parsel',
        'python-dateutil',
        'requests',
    ],
)
