from celery import Celery, current_task, group

import riberry
from riberry.app import RiberryApplication
from riberry.app.backends.impl.celery import CeleryBackend


# Create our Celery application
app = Celery(main='test.celery')
app.conf.update(riberry.config.config.celery)

# Create a Riberry backend from our Celery app
backend = CeleryBackend(instance=app)

# Create our Riberry application, specifying the application name and
rib = RiberryApplication(name='test.celery', backend=backend)

# Shortcut access to our Riberry context
cxt = rib.context


@rib.entry_point('test.celery.form.basic')
def entry_point():
    """
    Triggered when a new job execution is created for the
    'apps.celery.basic.form.basic' form.
    """

    # Extract "streams" input value
    stream_count = cxt.input.values['streams']

    chains = []
    for num in range(stream_count):
        # Create a Riberry stream using Celery's chain
        with cxt.flow.stream_scope(stream=f'Stream #{num}'):
            chain = cxt.flow.start(process).s(num) | cxt.flow.end(save).s(num)
            chains.append(chain)

    # Construct a workflow 'chord' which will process each input,
    # save each result, and then save all results into one file.
    workflow = group(*chains) | save_all.s()

    # Replace the current task with our workflow
    raise current_task.replace(workflow)


@rib.task(name='process')
def process(num: int) -> int:
    """ Processes our input value and returns the result. """

    return num * num


@rib.task(name='save')
def save(result: int, num: int):
    """ Saves the individual result and returns it for further use. """

    output = {'number': num, 'output': result}
    cxt.artifact.create(filename=f'{num}.txt', content=output)
    return output


@rib.task(name='save_all')
def save_all(results: list):
    """ Saves all results into a single file. """

    results.sort(key=lambda x: x['number'])
    cxt.artifact.create(filename=f'all.txt', content=results)
