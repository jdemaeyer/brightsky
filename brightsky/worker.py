import time

from huey import crontab, PriorityRedisHuey
from huey.api import TaskLock as TaskLock_
from huey.exceptions import TaskLockedException

from brightsky import tasks
from brightsky.settings import settings


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


@huey.task()
def process(url):
    with huey.lock_task(url):
        tasks.parse(url=url, export=True)


@huey.periodic_task(
    crontab(minute=settings.POLLING_CRONTAB_MINUTE), priority=50)
def poll():
    tasks.poll(enqueue=True)


@huey.periodic_task(crontab(minute='23'), priority=0)
def clean():
    tasks.clean()
