from flask_restplus import Namespace, Resource
from webargs.flaskparser import use_args

from riberry.rest import services, views
from . import args
from ..base import parse_args

api = Namespace('application-interfaces', description='Application interface resources')


@api.route('/', endpoint='application-interface:collection')
class ApplicationInterfaceCollectionResource(Resource):

    @use_args(args.base)
    def get(self, options):
        options = parse_args(options)
        return views.make_response(services.application_interface.all_application_interfaces(options))


@api.route('/<id>', endpoint='application-interface:resource')
class ApplicationInterfaceResource(Resource):

    @use_args(args.base)
    def get(self, options, id):
        options = parse_args(options)
        return views.make_response(
            services.application_interface.application_interface_by_id(application_interface_id=id, options=options))
