from setuptools import setup


with open('README.md') as f:
    long_description = f.read()

setup(
    name='brightsky',
    version='0.9.6',
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
        'falcon',
        'falcon-cors',
        'gunicorn',
        'huey[redis]',
        'parsel',
        'psycopg2-binary',
        'python-dateutil',
        'requests',
        'sentry-sdk',
    ],
)
