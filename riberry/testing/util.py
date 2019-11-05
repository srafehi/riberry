from typing import Callable, Iterable, Optional

import time


def run_until_successful(
        func: Callable,
        args: Optional[Iterable] = None,
        kwargs: Optional[dict] = None,
        time_limit: int = 10,
        timeout_assertion_message: Optional[str] = None,
        wait_interval: int = 1,
):
    """ Runs the given function at the given interval until it returns True or exceeds the time limit. """

    start_time = time.time()

    while True:
        if func(*args or (), **kwargs or {}):
            return
        assert time.time() - start_time < time_limit, timeout_assertion_message or (
                timeout_assertion_message or f'Function {func.__name__} failed'
        )
        time.sleep(wait_interval)
