from celery import bootsteps
import riberry
import time


class Addon:

    def register(self, riberry_app: 'riberry.celery.app.base.RiberryApplication'):
        raise NotImplementedError


class AddonStartStopStep(bootsteps.StartStopStep):
    requires = {'celery.worker.components:Timer'}

    rib: 'riberry.celery.app.base.RiberryApplication'

    def __init__(self, worker, interval, priority=10):

        self._timer = None
        self.interval = interval
        self.priority = priority
        self._worker = worker

    @property
    def worker(self):
        return self._worker

    @property
    def worker_state(self):
        return self.worker.state

    @property
    def consumer(self):
        return self.worker.consumer

    def start(self, worker):
        if self.should_run():
            self._timer = worker.timer.call_repeatedly(
                self.interval, self.__execute, priority=self.priority,
            )

    def stop(self, worker):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def __execute(self):
        start_time = time.time()
        try:
            self.run()
        finally:
            print(f'Completed {type(self).__name__} in {time.time() - start_time:2} seconds')

    def should_run(self) -> bool:
        raise NotImplementedError

    def run(self):
        raise NotImplementedError
