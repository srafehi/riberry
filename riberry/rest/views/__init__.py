from . import application, application_instance, form, application_interface, auth, job, self


def make_response(data):
    return {
        'data': data
    }