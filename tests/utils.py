from contextlib import contextmanager

from brightsky.settings import settings


def is_subset(subset_dict, full_dict):
    return subset_dict == {k: full_dict[k] for k in subset_dict}


@contextmanager
def overridden_settings(**overrides):
    original = {k: settings[k] for k, v in overrides.items() if k in settings}
    settings.update(overrides)
    try:
        yield
    finally:
        for k in overrides:
            del settings[k]
        settings.update(original)
