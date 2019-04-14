import types

import celery


def patch_send_task(instance: celery.Celery, func):
    send_task_original = instance.send_task
    def send_task(self, *args, **kwargs):
        func(self, *args, **kwargs)
        return send_task_original(*args, **kwargs)
    instance.send_task = types.MethodType(send_task, instance)

