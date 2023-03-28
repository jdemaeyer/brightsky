import logging
import resource
import threading
import time

from dwdparse.stations import load_stations
from huey import crontab, PriorityRedisHuey
from huey.api import TaskLock as TaskLock_
from huey.exceptions import TaskLockedException

from brightsky import tasks
from brightsky.settings import settings


logger = logging.getLogger(__name__)


class ExpiringLocksHuey(PriorityRedisHuey):

    def lock_task(self, lock_name):
        return TaskLock(self, lock_name)

    def expire_locks(self, timeout):
        expired = set()
        threshold = time.time() - timeout
        for key in list(self._locks):
            value = self.get(key, peek=True)
            if value and float(value) < threshold:
                self.delete(key)
                expired.add(key)
        return expired

    def is_locked(self, lock_name):
        return TaskLock(self, lock_name).is_locked()


class TaskLock(TaskLock_):

    def __enter__(self):
        if not self._huey.put_if_empty(self._key, str(time.time())):
            raise TaskLockedException('unable to set lock: %s' % self._name)

    def is_locked(self):
        return self._huey.storage.has_data_for_key(self._key)


huey = ExpiringLocksHuey(
    'brightsky',
    results=False,
    url=settings.REDIS_URL,
)


@huey.periodic_task(crontab(minute='42', hour='3'), priority=40)
@huey.on_startup()
def update_stations():
    with update_stations._lock:
        if time.time() - update_stations._last_update < 60:
            return
        load_stations()
        update_stations._last_update = time.time()
update_stations._lock = threading.Lock()  # noqa: E305
update_stations._last_update = 0


@huey.task()
def process(url):
    with huey.lock_task(url):
        tasks.parse(url)


@huey.periodic_task(
    crontab(minute=settings.POLLING_CRONTAB_MINUTE), priority=50)
def poll():
    tasks.poll(enqueue=True)


@huey.periodic_task(crontab(minute='23'), priority=0)
def clean():
    tasks.clean()


@huey.periodic_task(crontab(), priority=100)
def log_health():
    max_mem = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
    logger.info(f"Maximum memory usage: {max_mem:,} MiB")
