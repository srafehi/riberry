import json
import smtplib
import traceback
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

import pendulum
from sqlalchemy.orm.exc import NoResultFound

from riberry import model, config
from celery.utils.log import logger


def email_notification(host, body, mime_type, subject, sender, recipients: List):
    if not recipients:
        logger.warn('Attempted to send email notification with no recipients provided.')
        return

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ', '.join(recipients)
        msg.attach(MIMEText(body, mime_type))
        s = smtplib.SMTP(host)
        s.sendmail(sender, recipients, msg.as_string())
        s.quit()
    except:
        logger.warn(f'An error occurred while sending email notification: {traceback.format_exc()}')


def handle_artifacts(events: List[model.misc.Event]):
    job_executions = {}
    streams = {}

    to_delete = []
    for event in events:
        try:
            event_data = json.loads(event.data)
            stream_name = event_data['stream']

            artifact = model.job.JobExecutionArtifact(
                name=event_data['name'] or 'Untitled',
                type=model.job.ArtifactType[event_data['type']],
                category=event_data['category'] or 'Default',
                filename=event_data['filename'] or 'Untitled',
                size=len(event.binary) if event.binary else 0,
                created=pendulum.from_timestamp(event.time),
                binary=model.job.JobExecutionArtifactBinary(binary=event.binary),
                data=[
                    model.job.JobExecutionArtifactData(
                        title=str(title),
                        description=str(description)
                    ) for title, description in event_data['data'].items() if title and description
                ]
            )

            if event.root_id not in job_executions:
                try:
                    job_executions[event.root_id] = model.job.JobExecution.query().filter_by(task_id=event.root_id).one()
                except NoResultFound:
                    to_delete.append(event)
                    continue

            job_execution = job_executions[event.root_id]
            artifact.job_execution = job_executions[event.root_id]

            if stream_name:
                if (stream_name, event.root_id) not in streams:
                    try:
                        streams[(stream_name, event.root_id)] = model.job.JobExecutionStream.query().filter_by(
                            name=stream_name, job_execution=job_execution).one()
                    except NoResultFound:
                        to_delete.append(event)
                        continue
                stream = streams[(stream_name, event.root_id)]
                artifact.stream = stream

            model.conn.add(artifact)
        except:
            logger.warn(f'An error occurred processing artifact event {event}: {traceback.format_exc()}')
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
                try:
                    job_executions[event.root_id] = model.job.JobExecution.query().filter_by(
                        task_id=event.root_id).one()
                except NoResultFound:
                    to_delete.append(event)
                    continue
            job_execution = job_executions[event.root_id]

            if (stream_name, event.root_id) not in streams:
                try:
                    streams[(stream_name, event.root_id)] = model.job.JobExecutionStream.query().filter_by(
                        name=stream_name, job_execution=job_execution).one()
                except NoResultFound:
                    to_delete.append(event)
                    continue
            stream = streams[(stream_name, event.root_id)]

            try:
                if (stream_name, event.task_id) not in steps:
                    steps[(stream_name, event.task_id)] = model.job.JobExecutionStreamStep.query().filter_by(
                        task_id=event.task_id, stream=stream).one()
                step = steps[(stream_name, event.task_id)]
            except NoResultFound:
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
                elif status in ('SUCCESS', 'FAILURE'):
                    step.completed = event_time

        except:
            logger.warn(f'An error occurred processing step event {event}: {traceback.format_exc()}')
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

            if not stream_name:
                logger.warn('Empty stream name provided, skipping')
                to_delete.append(event)
                continue

            if event.root_id not in job_executions:
                try:
                    job_executions[event.root_id] = model.job.JobExecution.query().filter_by(
                        task_id=event.root_id).one()
                except NoResultFound:
                    to_delete.append(event)
                    continue
            job_execution = job_executions[event.root_id]
            try:
                if (stream_name, event.root_id) not in streams:
                    streams[(stream_name, event.root_id)] = model.job.JobExecutionStream.query().filter_by(
                        name=stream_name, job_execution=job_execution).one()
                stream = streams[(stream_name, event.root_id)]
            except NoResultFound:
                existing_stream = model.job.JobExecutionStream.query().filter_by(task_id=event.task_id).first()
                if existing_stream:
                    logger.warn(f'Skipping stream event {event}. Task ID {event.task_id!r} already exists against '
                                f'an existing stream (id={existing_stream.id}).\n'
                                f'Details:\n'
                                f'  root_id: {event.root_id!r}\n'
                                f'  name: {stream_name!r}\n'
                                f'  data: {event_data}\n')
                    to_delete.append(event)
                    continue

                stream = model.job.JobExecutionStream(
                    name=str(event_data['stream']),
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
            logger.warn(f'An error occurred processing stream event {event}: {traceback.format_exc()}')
        else:
            to_delete.append(event)

    return to_delete


def handle_notifications(events: List[model.misc.Event]):
    to_delete = []

    for event in events:
        event_data = json.loads(event.data)
        notification_type = event_data['type']
        notification_data = event_data['data']

        try:
            execution: model.job.JobExecution = model.job.JobExecution.query().filter_by(task_id=event.root_id).one()
            user = execution.creator
        except NoResultFound:
            to_delete.append(event)
            continue

        if notification_type == 'custom-email' and config.config.email.enabled:
            try:
                email_notification(
                    host=config.config.email.smtp_server,
                    body=notification_data['body'],
                    mime_type=notification_data.get('mime_type') or 'plain',
                    subject=notification_data['subject'],
                    sender=notification_data.get('from') or config.config.email.sender,
                    recipients=list(filter(None, [user.details.email] + notification_data.get('to', []))),
                )
            except:
                logger.warn(f'An error occurred processing notification type {notification_type}: '
                            f'{traceback.format_exc()}')

        elif notification_type == 'workflow_complete':
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
            if config.config.email.enabled and user.details.email:
                email_notification(
                    host=config.config.email.smtp_server,
                    body=message,
                    mime_type='plain',
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
            if config.config.email.enabled and user.details.email:
                email_notification(
                    host=config.config.email.smtp_server,
                    body=message,
                    mime_type='plain',
                    subject=f'Riberry / Started / {execution.job.name} / execution #{execution.id}',
                    sender=config.config.email.sender,
                    recipients=[user.details.email],
                )
        else:
            logger.warn(f'Received unknown notification type {notification_type}')

        to_delete.append(event)

    return to_delete


handlers = {
    'stream': handle_streams,
    'step': handle_steps,
    'artifact': handle_artifacts,
    'notify': handle_notifications,
}


def process(event_limit=None):
    with model.conn:
        event_mapping = defaultdict(list)
        query = model.misc.Event.query().order_by(model.misc.Event.time.asc(), model.misc.Event.id.asc())
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
                    logger.warn(f'Failed to process {handler_name} events: {handler_events}')
                    raise

        for event in to_delete:
            logger.info(f'Removing processed event {event}')
            model.conn.delete(event)

        model.conn.commit()


if __name__ == '__main__':
    process()
