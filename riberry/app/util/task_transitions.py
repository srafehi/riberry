from typing import Optional

import riberry
from ..util.events import create_event

__stream_cache = {}


def try_cache_stream(root_id: str, stream_name: str, stream_state: str):
    key = root_id, stream_name, stream_state
    if key not in __stream_cache:
        __stream_cache[key] = None
        if len(__stream_cache) > 10000:
            evict_key = next(iter(__stream_cache))
            __stream_cache.pop(evict_key)
        return True
    return False


def task_created(context, task_id: str, stream: Optional[str], step: Optional[str], props: dict):
    try:
        root_id = context.current.root_id
    except:
        return

    if props.get('stream_start'):
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

    if step and riberry.app.current_riberry_app.config.enable_steps:
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


def task_active(context, props: dict):
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


def task_complete(context, props: dict, state: str):
    task_id = context.current.task_id
    root_id = context.current.root_id

    stream = context.current.stream
    step = context.current.step

    if not stream:
        return

    if props.get('stream_start') and state in ('RETRY', 'FAILURE'):
        create_event(
            name='stream',
            root_id=root_id,
            task_id=task_id,
            data={
                'stream': str(stream),
                'state': state,
            }
        )

    if props.get('stream_end'):
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
