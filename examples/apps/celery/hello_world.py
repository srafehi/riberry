import celery

from riberry.app import RiberryApplication
from riberry.app.backends.impl.celery import CeleryBackend

# create a celery application
app = celery.Celery(...)

# create a riberry application with celery as our backend
rib = RiberryApplication(
    name='sample_app',                    # riberry application name
    backend=CeleryBackend(instance=app),  # riberry backend
)

# easy access to our riberry context
cxt = rib.context


# register an entry point for one of our application's forms
@rib.entry_point(form='sample_app.form.sample', stream='Sample Stream')
def sample_form():

    # extract the username
    username = cxt.current.job_execution.creator.username
    cxt.artifact.create(filename='sample.txt', content=f'Hello {username}')
