from . import application, application_instance, instance_interface, application_interface, auth, job


def make_response(data):
    return {
        'data': data
    }