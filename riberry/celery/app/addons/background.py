from .base import Addon, AddonStartStopStep
import riberry


class BackgroundTasks(Addon):

    def register(self, riberry_app: 'riberry.celery.app.base.RiberryApplication'):
        class ConcreteBackgroundTasksStep(BackgroundTasksStep):
            rib = riberry_app

        riberry_app.celery_app.steps['worker'].add(ConcreteBackgroundTasksStep)


class BackgroundTasksStep(AddonStartStopStep):
    requires = {'celery.worker.components:Timer'}

    def __init__(self, worker, **_):
        super().__init__(worker=worker, interval=0.6)
        self.lock = riberry.celery.app.util.redis_lock.RedisLock(name='step:background', on_acquired=self.on_lock_acquired, interval=500)

    def on_lock_acquired(self):
        try:
            riberry.celery.app.tasks.echo()
            riberry.celery.app.tasks.poll()
            riberry.celery.app.tasks.refresh()
        finally:
            riberry.model.conn.remove()
            riberry.model.conn.raw_engine.dispose()

    def should_run(self) -> bool:
        return True

    def run(self):
        redis_instance = riberry.celery.util.celery_redis_instance()
        self.lock.run(redis_instance=redis_instance)
