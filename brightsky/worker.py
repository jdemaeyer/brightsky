import os

from huey import crontab, RedisHuey

from brightsky import tasks
from brightsky.utils import configure_logging


huey = RedisHuey('brightsky', results=False, url=os.getenv('REDIS_URL'))


@huey.on_startup()
def startup():
    configure_logging()
    huey.flush()


@huey.task()
def process(url):
    with huey.lock_task(url):
        tasks.parse(url=url, export=True)


@huey.periodic_task(crontab(minute='*'))
def poll():
    tasks.poll(enqueue=True)
