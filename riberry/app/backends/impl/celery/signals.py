from celery import signals

import riberry


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
    """ Inform Riberry of newly created tasks. """
    
    _, task_input, *_ = body

    riberry.app.util.task_transitions.task_created(
        context=riberry.app.current_context,
        task_id=headers['id'],
        stream=task_input.get('__rib_stream'),
        step=task_input.get('__rib_step'),
        props=dict(
            stream_start=task_input.get('__rib_stream_start') or False,
        ),
    )
