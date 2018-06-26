import json
import smtplib
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

import pendulum

from riberry import model, config


def email_notification(host, body, subject, sender, recipients: List):
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ', '.join(recipients)
        msg.attach(MIMEText(body, 'plain'))
        s = smtplib.SMTP(host)
        s.sendmail(sender, recipients, msg.as_string())
        s.quit()
    except:
        pass


def handle_artifacts(events: List[model.misc.Event]):
    job_executions = {}
    streams = {}

    to_delete = []
    for event in events:
        try:
            event_data = json.loads(event.data)
            stream_name = event_data['stream']

            artifact = model.job.JobExecutionArtifact(
                name=event_data['name'],
                type=model.job.ArtifactType[event_data['type']],
                category=event_data['category'],
                filename=event_data['filename'],
                size=len(event.binary),
                created=pendulum.from_timestamp(event.time),
                binary=model.job.JobExecutionArtifactBinary(binary=event.binary),
                data=[
                    model.job.JobExecutionArtifactData(
                        title=title,
                        description=description
                    ) for title, description in event_data['data'].items()
                ]
            )

            if event.root_id not in job_executions:
                job_executions[event.root_id] = model.job.JobExecution.query().filter_by(task_id=event.root_id).one()
            job_execution = job_executions[event.root_id]
            artifact.job_execution = job_executions[event.root_id]

            if stream_name:
                if (stream_name, event.root_id) not in streams:
                    streams[(stream_name, event.root_id)] = model.job.JobExecutionStream.query().filter_by(
                        name=stream_name, job_execution=job_execution).one()
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
                streams[(stream_name, event.root_id)] = model.job.JobExecutionStream.query().filter_by(name=stream_name,
                                                                                                       job_execution=job_execution).one()
            stream = streams[(stream_name, event.root_id)]

            try:
                if (stream_name, event.task_id) not in steps:
                    steps[(stream_name, event.task_id)] = model.job.JobExecutionStreamStep.query().filter_by(
                        task_id=event.task_id, stream=stream).one()
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

            step_updated = pendulum.instance(step.updated, tz='utc')
            if event_time >= step_updated:
                status = event_data['state']
                step.status = status
                step.updated = event_time
                if status == 'ACTIVE':
                    step.started = event_time
                elif status in ('SUCCESS', 'FAILURED'):
                    step.completed = event_time


        except:
            import traceback
            print(traceback.format_exc())
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
                    streams[(stream_name, event.root_id)] = model.job.JobExecutionStream.query().filter_by(
                        name=stream_name, job_execution=job_execution).one()
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

            stream_updated = pendulum.instance(stream.updated, tz='utc')
            if event_time >= stream_updated:
                status = event_data['state']
                stream.status = status
                stream.updated = event_time
                if status == 'ACTIVE' and stream.started is None:
                    stream.started = event_time
                elif status in ('SUCCESS', 'FAILURE'):
                    stream.completed = event_time
        except:
            pass
        else:
            to_delete.append(event)

    return to_delete


def handle_notifications(events: List[model.misc.Event]):
    to_delete = []

    for event in events:
        event_data = json.loads(event.data)
        notification_type = event_data['type']
        notification_data = event_data['data']

        execution: model.job.JobExecution = model.job.JobExecution.query().filter_by(task_id=event.root_id).one()
        user = execution.creator

        if notification_type == 'workflow_complete':
            status = str(notification_data['status']).lower()
            message = f'Completed execution #{execution.id} for job ' \
                      f'{execution.job.name} with status {str(status).lower()}'
            notification = model.misc.Notification(
                type=(
                    model.misc.NotificationType.success if str(status).lower() == 'success'
                    else model.misc.NotificationType.error
                ),
                message=message,
                user_notifications=[
                    model.misc.UserNotification(user=user)
                ],
                targets=[
                    model.misc.NotificationTarget(target='JobExecution', target_id=execution.id)
                ]
            )
            model.conn.add(notification)
            if config.config.email.enabled:
                email_notification(
                    host=config.config.email.smtp_server,
                    body=message,
                    subject=f'Riberry / {status.title()} / {execution.job.name} / execution #{execution.id}',
                    sender=config.config.email.sender,
                    recipients=[user.details.email],
                )

        elif notification_type == 'workflow_started':
            message = f'Processing execution #{execution.id} for job {execution.job.name}'
            notification = model.misc.Notification(
                type=model.misc.NotificationType.info,
                message=message,
                user_notifications=[
                    model.misc.UserNotification(user=execution.creator)
                ],
                targets=[
                    model.misc.NotificationTarget(target='JobExecution', target_id=execution.id)
                ]
            )
            model.conn.add(notification)
            if config.config.email.enabled:
                email_notification(
                    host=config.config.email.smtp_server,
                    body=message,
                    subject=f'Riberry / Started / {execution.job.name} / execution #{execution.id}',
                    sender=config.config.email.sender,
                    recipients=[user.details.email],
                )

        to_delete.append(event)

    return to_delete


handlers = {
    'stream': handle_streams,
    'step': handle_steps,
    'artifact': handle_artifacts,
    'notify': handle_notifications,
}


def process(event_limit=None):
    try:
        event_mapping = defaultdict(list)
        query = model.misc.Event.query().order_by(model.misc.Event.time.asc())
        if event_limit:
            query = query.limit(event_limit)
        events = query.all()

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