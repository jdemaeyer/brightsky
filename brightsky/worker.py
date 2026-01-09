import logging
import pathlib
import resource
import threading
import time

from dwdparse.stations import StationIDConverter, load_stations
from dwdparse.utils import fetch
from huey import crontab, PriorityRedisHuey
from huey.api import TaskLock as TaskLock_
from huey.exceptions import TaskLockedException

from brightsky import tasks
from brightsky.settings import settings


logger = logging.getLogger(__name__)


class ExpiringLocksHuey(PriorityRedisHuey):

    def lock_task(self, lock_name):
        """Return a TaskLock for `lock_name` to be used as a context manager."""
        return TaskLock(self, lock_name)

    def expire_locks(self, timeout):
        """Expire internal locks older than `timeout` seconds and return them."""
        expired = set()
        threshold = time.time() - timeout
        for key in list(self._locks):
            value = self.get(key, peek=True)
            if value and float(value) < threshold:
                self.delete(key)
                expired.add(key)
        return expired

    def is_locked(self, lock_name):
        """Return whether `lock_name` is currently locked."""
        return TaskLock(self, lock_name).is_locked()


class TaskLock(TaskLock_):

    def __enter__(self):
        """Acquire the task lock or raise TaskLockedException."""
        if not self._huey.put_if_empty(self._key, str(time.time())):
            raise TaskLockedException('unable to set lock: %s' % self._name)

    def is_locked(self):
        """Check whether this TaskLock is currently set."""
        return self._huey.storage.has_data_for_key(self._key)


huey = ExpiringLocksHuey(
    'brightsky',
    results=False,
    url=settings.REDIS_URL,
)


@huey.periodic_task(crontab(minute='42', hour='3'), priority=40)
@huey.on_startup()
def update_stations():
    """Periodically fetch and cache the DWD station list."""
    path = pathlib.Path('.cache', 'stations.html')
    with update_stations._lock:
        if time.time() - update_stations._last_update < 60:
            return
        # On startup, skip download and load from cache if available
        if not update_stations._last_update and path.is_file():
            load_stations(path=path)
        else:
            # Fetch station list first so network errors are raised immediately
            try:
                station_list_content = fetch(StationIDConverter.STATION_LIST_URL)
            except Exception as e:
                logger.error("Failed to fetch station list: %s", e)
                raise

            # Try to cache the fetched content, but don't fail if caching
            # is not possible (e.g., permission errors). In that case we
            # still have the fetched content and can fall back to loading
            # it directly.
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, 'wb') as f:
                    f.write(station_list_content)
            except (IOError, PermissionError) as e:
                logger.warning("Cannot cache station list: %s", e)
                # load_stations() will fetch again if necessary; network
                # should be available because we fetched successfully.
                load_stations()
            else:
                load_stations(path=path)
        update_stations._last_update = time.time()
update_stations._lock = threading.Lock()  # noqa: E305
update_stations._last_update = 0


@huey.task()
def process(url):
    """Huey task that wraps `tasks.parse` for the given file `url`."""
    with huey.lock_task(url):
        tasks.parse(url)


@huey.periodic_task(
    crontab(minute=settings.POLLING_CRONTAB_MINUTE), priority=50)
def poll():
    """Periodic task to poll DWD and enqueue updated files for parsing."""
    tasks.poll(enqueue=True)


@huey.periodic_task(crontab(minute='23'), priority=0)
def clean():
    """Periodic cleanup task that removes expired DB records."""
    tasks.clean()


@huey.periodic_task(crontab(), priority=100)
def log_health():
    """Log a simple health metric (max memory usage)."""
    max_mem = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
    logger.info(f"Maximum memory usage: {max_mem:,} MiB")
