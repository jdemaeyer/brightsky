import os
from contextlib import contextmanager
from functools import partial

from brightsky.settings import settings as bs_settings


def is_subset(subset_dict, full_dict):
    return subset_dict == {k: full_dict[k] for k in subset_dict}


@contextmanager
def dict_override(d, **overrides):
    original = {k: d[k] for k, v in d.items() if k in d}
    d.update(overrides)
    try:
        yield
    finally:
        for k in overrides:
            del d[k]
        d.update(original)


settings = partial(dict_override, bs_settings)
environ = partial(dict_override, os.environ)
