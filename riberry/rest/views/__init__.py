from . import application, application_instance, instance_interface, application_interface, auth


def make_response(data):
    return {
        'data': data
    }