from typing import Optional, List

from .. import current_context as context
from ..util.events import create_event


def notify(notification_type, data=None, task_id=None, root_id=None):
    task_id = task_id or context.current.task_id
    root_id = root_id or context.current.root_id

    create_event(
        'notify',
        root_id=root_id,
        task_id=task_id,
        data={
            'type': notification_type,
            'data': data or {}
        },
        binary=None
    )


def workflow_complete(task_id: str, root_id: str, status: str):
    notify(
        notification_type='workflow_complete',
        data=dict(status=status),
        task_id=task_id,
        root_id=root_id
    )


def send_email(
        subject: str,
        body: str,
        mime_type: Optional[str] = None,
        sender: Optional[str] = None,
        receivers: Optional[List[str]] = None
):
    if isinstance(receivers, str):
        receivers = [receivers]

    notify(
        notification_type='custom-email',
        data={
            'subject': subject,
            'mime_type': mime_type,
            'body': body,
            'from': sender,
            'to': receivers or []
        }
    )
