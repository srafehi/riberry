from .base import AddonStartStopStep
import riberry


class BackgroundTasks(riberry.app.addons.Addon):

    def register(self, riberry_app: 'riberry.app.base.RiberryApplication'):
        class ConcreteBackgroundTasksStep(BackgroundTasksStep):
            rib = riberry_app

        riberry_app.backend.steps['worker'].add(ConcreteBackgroundTasksStep)


class BackgroundTasksStep(AddonStartStopStep):
    requires = {'celery.worker.components:Timer'}

    def __init__(self, worker, **_):
        super().__init__(worker=worker, interval=1.0)
        self.lock = riberry.app.util.redis_lock.RedisLock(name='step:background', on_acquired=self.on_lock_acquired, interval=900)

    @staticmethod
    def on_lock_acquired():
        try:
            riberry.app.tasks.echo()
            riberry.app.tasks.poll()
            riberry.app.tasks.refresh()
        finally:
            riberry.model.conn.remove()
            riberry.model.conn.raw_engine.dispose()

    def should_run(self) -> bool:
        return True

    def run(self):
        redis_instance = riberry.celery.util.celery_redis_instance()
        self.lock.run(redis_instance=redis_instance)
