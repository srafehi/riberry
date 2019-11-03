from celery import signals

import riberry
from ..util.events import create_event


log = riberry.log.make(__name__)
__stream_cache = {}


def try_cache_stream(root_id, stream_name, stream_state):
    key = root_id, stream_name, stream_state
    if key not in __stream_cache:
        __stream_cache[key] = None
        if len(__stream_cache) > 10000:
            evict_key = next(iter(__stream_cache))
            __stream_cache.pop(evict_key)
        return True
    return False


@signals.celeryd_after_setup.connect
def celeryd_after_setup(**_):
    """
    Dispose of Riberry's SQLAlchemy engine after the Celery daemon has completed setup.
    """
    riberry.model.conn.dispose_engine()


@signals.worker_process_init.connect
def worker_process_init(*args, **kwargs):
    """
    Dispose of Riberry's SQLAlchemy engine in newly initialized worker processes.

    This is used to prevent database sessions being mistakenly used by both
    the parent and child process.
    """

    riberry.model.conn.dispose_engine()


@signals.worker_ready.connect
def worker_ready(sender, **_):
    """
    Patch "celery.concurrency.asynpool.AsynPool.on_process_up" to dispose of
    Riberry's SQLAlchemy engine whenever a new worker process is created via
    prefork.

    This is used to prevent database sessions being mistakenly used by both
    the parent and child process.
    """

    pool = getattr(sender.pool, '_pool', None)
    if pool is None:
        return

    on_process_up_original = pool.on_process_up

    def on_process_up(proc):
        riberry.model.conn.dispose_engine()
        on_process_up_original(proc)

    pool.on_process_up = on_process_up
    riberry.model.conn.dispose_engine()


@signals.before_task_publish.connect
def before_task_publish(sender, headers, body, **_):
    try:
        root_id = riberry.app.current_context.current.root_id
    except:
        return

    args, kwargs, *_ = body
    task_id = headers['id']

    if '__rib_stream_start' in kwargs:
        stream = str(kwargs['__rib_stream'])
        if try_cache_stream(root_id=root_id, stream_name=stream, stream_state='QUEUED'):
            create_event(
                name='stream',
                root_id=root_id,
                task_id=task_id,
                data={
                    'stream': stream,
                    'state': 'QUEUED',
                }
            )

    if '__rib_step' in kwargs and riberry.app.current_riberry_app.config.enable_steps:
        stream, step = kwargs['__rib_stream'], kwargs['__rib_step']
        create_event(
            name='step',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': str(stream),
                'step': str(step),
                'state': 'QUEUED',
            }
        )


def task_prerun(context, props):
    task_id = context.current.task_id
    root_id = context.current.root_id

    stream = context.current.stream
    step = context.current.step

    if not stream:
        return

    if 'stream_start' in props:
        if try_cache_stream(root_id=root_id, stream_name=stream, stream_state='ACTIVE'):
            create_event(
                name='stream',
                root_id=root_id,
                task_id=task_id,
                data={
                    'stream': str(stream),
                    'state': 'ACTIVE',
                }
            )

    if step and riberry.app.current_riberry_app.config.enable_steps:
        create_event(
            name='step',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': str(stream),
                'step': str(step),
                'state': 'ACTIVE',
            }
        )


def task_postrun(context, props, state):
    task_id = context.current.task_id
    root_id = context.current.root_id

    stream = context.current.stream
    step = context.current.step

    if not stream:
        return

    if 'stream_start' in props and state in ('RETRY', 'FAILURE'):
        create_event(
            name='stream',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': str(stream),
                'state': state,
            }
        )

    if 'stream_end' in props:
        create_event(
            name='stream',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': str(stream),
                'state': 'SUCCESS' if state == 'IGNORED' else state or 'FAILURE',
            }
        )

    if step and riberry.app.current_riberry_app.config.enable_steps:
        create_event(
            name='step',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': str(stream),
                'step': str(step),
                'state': 'SUCCESS' if state == 'IGNORED' else state or 'FAILURE',
            }
        )
