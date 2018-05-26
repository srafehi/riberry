from . import application, application_instance, form, application_interface, auth, job


def make_response(data):
    return {
        'data': data
    }