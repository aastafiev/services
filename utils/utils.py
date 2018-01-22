import yaml
import warnings
import functools
import inspect


def load_cfg(path):
    if not isinstance(path, str):
        raise TypeError('Path set incorrect. Expected string - full path to config file')

    with open(path, 'r') as conf_stream:
        config = yaml.safe_load(conf_stream)

    return config


def ignore_warnings(func):
    @functools.wraps(func)
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            func(self, *args, **kwargs)

    @functools.wraps(func)
    async def do_test_async(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            await func(self, *args, **kwargs)

    return do_test_async if inspect.iscoroutinefunction(func) else do_test
