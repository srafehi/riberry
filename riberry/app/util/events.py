import json

import pendulum

import riberry


def create_event(name, root_id, task_id, data=None, binary=None):
    if not root_id:
        return

    if isinstance(binary, str):
        binary = binary.encode()

    evt = riberry.model.misc.Event(
        name=name,
        time=pendulum.DateTime.utcnow().timestamp(),
        task_id=task_id,
        root_id=root_id,
        data=json.dumps(data),
        binary=binary,
    )

    riberry.model.conn.add(evt)
    riberry.model.conn.commit()
    riberry.model.conn.flush([evt])
