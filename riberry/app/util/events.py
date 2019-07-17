import json
import traceback

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

    try:
        riberry.model.conn.add(evt)
        riberry.model.conn.commit()
        riberry.model.conn.flush([evt])
    except:
        traceback.print_exc()
        riberry.model.conn.rollback()
