from flask_restplus import Namespace, Resource
from webargs.flaskparser import use_args

from riberry.rest import services, views
from . import args
from ..base import parse_args

api = Namespace('application-instances', description='Application instance resources')


@api.route('/', endpoint='application-instance:collection')
class ApplicationInstanceCollectionResource(Resource):

    @use_args(args.base)
    def get(self, options):
        options = parse_args(options)
        return views.make_response(services.application_instance.all_application_instances(options))


@api.route('/<id>', endpoint='application-instance:resource')
class ApplicationInstanceResource(Resource):

    @use_args(args.base)
    def get(self, options, id):
        options = parse_args(options)
        return views.make_response(
            services.application_instance.application_instance_by_id(application_instance_id=id, options=options))
