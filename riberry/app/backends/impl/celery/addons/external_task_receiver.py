from .base import AddonStartStopStep
import riberry
from riberry.app.backends.impl import celery as celery_backend


class ExternalTaskReceiver(riberry.app.addons.Addon):

    RECEIVER_QUEUE = 'rib.external'

    def register(self, riberry_app: 'riberry.app.base.RiberryApplication'):
        class ConcreteExternalTaskReceiverStep(ExternalTaskReceiverStep):
            rib = riberry_app

        riberry_app.backend.steps['worker'].add(ConcreteExternalTaskReceiverStep)
        riberry_app.backend.user_options['worker'].add(self.regiser_user_options)
        task_routes = {
            celery_backend.CeleryBackend.CHECK_EXTERNAL_TASK_NAME: {'queue': self.RECEIVER_QUEUE},
        }

        if not riberry_app.backend.conf.task_routes:
            riberry_app.backend.conf.task_routes = {}
        riberry_app.backend.conf.task_routes.update(task_routes)

        for addon in riberry_app.addons.values():
            if isinstance(addon, celery_backend.addons.Scale):
                addon.conf.ignore_queues.add(self.RECEIVER_QUEUE)

    @staticmethod
    def regiser_user_options(parser):
        parser.add_argument(
            '--rib-receiver', action='store_true', default=False,
            help='Receiver of external Riberry tasks.',
        )


class ExternalTaskReceiverStep(AddonStartStopStep):

    def __init__(self, worker, rib_receiver, **_):
        super().__init__(worker=worker, interval=1)
        self._is_receiver = bool(rib_receiver)

    def should_run(self) -> bool:
        return self._is_receiver

    def run(self):
        active = riberry.model.job.JobExecution.query().filter_by(
            status='ACTIVE'
        ).join(riberry.model.job.Job).filter_by(
            instance=self.rib.context.current.riberry_app_instance,
        ).join(riberry.model.job.JobExecutionExternalTask).filter_by(
            status='READY'
        ).count()

        operation = 'add' if active else 'cancel'

        consumer = self.worker.consumer
        queues = {q.name for q in consumer.task_consumer.queues}

        if operation == 'add' and ExternalTaskReceiver.RECEIVER_QUEUE not in queues:
            consumer.add_task_queue(ExternalTaskReceiver.RECEIVER_QUEUE)

        if operation == 'cancel' and ExternalTaskReceiver.RECEIVER_QUEUE in queues:
            consumer.cancel_task_queue(ExternalTaskReceiver.RECEIVER_QUEUE)
