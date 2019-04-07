import types

import celery


def patch_send_task(instance: celery.Celery):
    send_task_original = instance.send_task
    def send_task(self, *args, **kwargs):
        riberry_properties = {}
        if kwargs.get('kwargs'):
            for key, value in kwargs['kwargs'].items():
                if key.startswith('__rib_'):
                    riberry_properties[key.replace('__rib_', '', 1)] = value

        return send_task_original(*args, **kwargs)
    instance.send_task = types.MethodType(send_task, instance)

