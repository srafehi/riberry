from typing import List
from riberry import model
import json
from collections import defaultdict
import pendulum


def handle_artefacts(events: List[model.misc.Event]):
    job_executions = {}
    streams = {}

    to_delete = []
    for event in events:
        try:
            event_data = json.loads(event.data)
            stream_name = event_data['stream']

            artifact = model.job.JobExecutionArtifact(
                name=event_data['name'],
                filename=event_data['filename'],
                size=len(event.binary),
                created=pendulum.from_timestamp(event.time),
                binary=model.job.JobExecutionArtifactBinary(binary=event.binary)
            )

            if event.root_id not in job_executions:
                job_executions[event.root_id] = model.job.JobExecution.query().filter_by(task_id=event.root_id).one()
            job_execution = job_executions[event.root_id]
            artifact.job_execution = job_executions[event.root_id]

            if stream_name:
                if (stream_name, event.root_id) not in streams:
                    streams[(stream_name, event.root_id)] = model.job.JobExecutionStream.query().filter_by(name=stream_name, job_execution=job_execution).one()
                stream = streams[(stream_name, event.root_id)]
                artifact.stream = stream

            model.conn.add(artifact)
        except Exception as exc:
            print(exc)
            pass
        else:
            to_delete.append(event)

    return to_delete


def handle_steps(events: List[model.misc.Event]):
    to_delete = []
    steps = {}
    streams = {}
    job_executions = {}

    for event in events:
        try:
            event_data = json.loads(event.data)
            event_time = pendulum.from_timestamp(event.time)
            stream_name = event_data['stream']

            if event.root_id not in job_executions:
                job_executions[event.root_id] = model.job.JobExecution.query().filter_by(task_id=event.root_id).one()
            job_execution = job_executions[event.root_id]

            if (stream_name, event.root_id) not in streams:
                streams[(stream_name, event.root_id)] = model.job.JobExecutionStream.query().filter_by(name=stream_name, job_execution=job_execution).one()
            stream = streams[(stream_name, event.root_id)]

            try:
                if (stream_name, event.task_id) not in steps:
                    steps[(stream_name, event.task_id)] = model.job.JobExecutionStreamStep.query().filter_by(task_id=event.task_id, stream=stream).one()
                step = steps[(stream_name, event.task_id)]
            except Exception:
                step = model.job.JobExecutionStreamStep(
                    name=event_data['step'],
                    created=pendulum.from_timestamp(event.time),
                    updated=pendulum.from_timestamp(event.time),
                    task_id=event.task_id,
                    stream=stream,
                    status=event_data['state']
                )
                model.conn.add(step)

            step_updated = pendulum.instance(step.updated, tz='Australia/Melbourne')
            if event_time >= step_updated:
                step.status = event_data['state']
                step.updated = event_time
        except:
            pass
        else:
            to_delete.append(event)

    return to_delete


def handle_streams(events: List[model.misc.Event]):
    to_delete = []
    streams = {}
    job_executions = {}

    for event in events:
        try:
            event_data = json.loads(event.data)
            event_time = pendulum.from_timestamp(event.time, tz='utc')
            stream_name = event_data['stream']
            if event.root_id not in job_executions:
                job_executions[event.root_id] = model.job.JobExecution.query().filter_by(task_id=event.root_id).one()
            job_execution = job_executions[event.root_id]
            try:
                if (stream_name, event.root_id) not in streams:
                    streams[(stream_name, event.root_id)] = model.job.JobExecutionStream.query().filter_by(name=stream_name, job_execution=job_execution).one()
                stream = streams[(stream_name, event.root_id)]
            except Exception:
                stream = model.job.JobExecutionStream(
                    name=event_data['stream'],
                    task_id=event.task_id,
                    created=pendulum.from_timestamp(event.time, tz='utc'),
                    updated=pendulum.from_timestamp(event.time, tz='utc'),
                    status='QUEUED',
                    job_execution=job_execution
                )
                model.conn.add(stream)

            stream_updated = pendulum.instance(stream.updated, tz='Australia/Melbourne')
            if event_time >= stream_updated:
                stream.status = event_data['state']
                stream.updated = event_time
        except:
            pass
        else:
            to_delete.append(event)

    return to_delete


handlers = {
    'stream': handle_streams,
    'step': handle_steps,
    'artefact': handle_artefacts
}


def process():
    try:
        event_mapping = defaultdict(list)
        events = model.misc.Event.query().order_by(model.misc.Event.time.asc()).all()
        if not events:
            return

        for event in events:
            event_mapping[event.name].append(event)

        to_delete = []
        for handler_name, handler_func in handlers.items():
            handler_events = event_mapping[handler_name]
            if handler_events:
                try:
                    to_delete += handlers[handler_name](handler_events)
                except:
                    print(f'{handler_name} {handler_events}')
                    raise

        for event in to_delete:
            print(event)
            model.conn.delete(event)

        model.conn.commit()
    finally:
        model.conn.remove()


if __name__ == '__main__':
    process()
