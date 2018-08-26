import os

import time

from riberry import model, services
from riberry.celery.background.events import events
from riberry.celery.client.tasks import poll


def execute_riberry_job(workflow, instance_name, interface_name, interface_version):
    os.environ['RIBERRY_INSTANCE'] = 'instance'

    user = model.auth.User(
        username='johnsmith',
        details=model.auth.UserDetails(
            email='johnsmith@fake.domain.name',
        )
    )
    instance = model.application.ApplicationInstance(
        name='Dummy',
        internal_name=instance_name,
    )
    interface = model.interface.ApplicationInterface(
        name='Dummy',
        internal_name=interface_name,
        version=interface_version,
    )
    form = model.interface.Form(
        instance=instance,
        interface=interface
    )
    app = model.application.Application(
        name='Dummy',
        internal_name=workflow.name,
        type='Mock',
        instances=[instance],
        interfaces=[interface],
    )

    model.conn.add(app)

    model.conn.commit()

    j = services.job.create_job(
        form_id=form.id,
        name='Dummy',
        input_values={},
        input_files={},
        execute=True
    )
    j.creator = user
    execution = j.executions[0]
    execution.creator = user

    instance.heartbeat = model.application.Heartbeat()
    model.conn.commit()

    exec_id = execution.id
    poll()

    model.conn.commit()

    while True:

        if model.job.JobExecutionStream.query().filter_by(status='FAILURE').first():
            break

        main_stream = model.job.JobExecutionStream.query().filter_by(name='Overall').first()
        if main_stream and main_stream.status in ('SUCCESS', 'FAILURE'):
            break

        time.sleep(2)
        events.process()

    return exec_id
