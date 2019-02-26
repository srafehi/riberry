import os
from celery.worker import control


@control.control_command()
def toggle_external_task_queue(state, operation):
    if os.environ.get('RIBERRY_EXTERNAL_TASK'):

        queues = {q.name for q in state.consumer.task_consumer.queues}
        if operation == 'add' and 'rib.manual' not in queues:
            state.consumer.add_task_queue('rib.manual')
            return True

        if operation == 'cancel' and 'rib.manual' in queues:
            state.consumer.cancel_task_queue('rib.manual')
            return True

    return None
