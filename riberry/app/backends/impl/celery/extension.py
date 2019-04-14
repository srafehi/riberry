import celery
import riberry


class RiberryTask(celery.Task):

    def signature(self, args=None, *starargs, **starkwargs):
        sig = celery.Task.signature(self, args, *starargs, **starkwargs)
        context = riberry.app.current_context
        stream = sig.get('kwargs', {}).get('__rib_stream') or context.flow.scoped_stream or context.current.stream

        if stream:
            sig['kwargs']['__rib_stream'] = stream
            if '__rib_step' not in sig['kwargs']:
                sig['kwargs']['__rib_step'] = self.name

        return sig
