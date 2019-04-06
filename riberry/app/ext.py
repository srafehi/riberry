import celery
from . import current_context


class Celery(celery.Celery):

    def send_task(self, *args, **kwargs):
        riberry_properties = {}
        if kwargs.get('kwargs'):
            for key, value in kwargs['kwargs'].items():
                if key.startswith('__rib_'):
                    riberry_properties[key.replace('__rib_', '', 1)] = value

        return super().send_task(*args, **kwargs)


class RiberryTask(celery.Task):

    def signature(self, args=None, *starargs, **starkwargs):
        sig = celery.Task.signature(self, args, *starargs, **starkwargs)
        context = current_context
        stream = context.flow.scoped_stream or context.current.stream

        if stream:
            sig['kwargs']['__rib_stream'] = stream
            if '__rib_step' not in sig['kwargs']:
                sig['kwargs']['__rib_step'] = self.name

        return sig
