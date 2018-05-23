from typing import List, Dict

from riberry import model, services


class Expansion:

    def __init__(self, key, model_cls, uselist, alias=None):
        self.key = key
        self.model_cls = model_cls
        self.uselist = uselist
        self.alias = alias

    def should_expand(self, model: 'ViewModel') -> bool:
        return model.options and 'expand' in model.options and self.name in model.options['expand']

    def expand(self, model: 'ViewModel') -> Dict:
        relationship_options = {
            'expand': model.options['expand'][self.name] or {}
        }
        relationship = services.fetch_relationship(model.model, self.key, action='view')
        if self.uselist:
            if relationship:
                result = [self.model_cls(m, relationship_options).to_dict() for m in relationship]
            else:
                result = []
        else:
            if relationship is not None:
                result = self.model_cls(relationship, relationship_options).to_dict()
            else:
                result = None

        return {
            self.name: result
        }

    @property
    def name(self):
        return self.alias or self.key

    def __repr__(self):
        return f'<Expansion key={self.key!r} model_cls={self.model_cls.__name__!r}>'


class ViewModel:

    __model_mapping__ = {}

    def __init_subclass__(cls, **kwargs):
        cls.__model_mapping__[cls.__annotations__['model']] = cls

    def __init__(self, model, options=None):
        self.model = model
        self.options = options

    def to_dict(self):
        raise NotImplementedError

    @staticmethod
    def expansions() -> List[Expansion]:
        raise NotImplementedError

    def _resolve_expansions(self):
        mixin = {}
        expansions = self.expansions()
        for expansion in expansions:
            if expansion.should_expand(self):
                mixin.update(expansion.expand(self))
            else:
                if '_expansions' not in mixin:
                    mixin['_expansions'] = {}
                mixin['_expansions'].update({expansion.name: None})
        return mixin


class Application(ViewModel):

    model: model.application.Application

    @staticmethod
    def expansions() -> List[Expansion]:
        return [
            Expansion(key='instances', model_cls=ApplicationInstance, uselist=True),
            Expansion(key='interfaces', model_cls=ApplicationInterface, uselist=True),
            Expansion(key='instance_interfaces', model_cls=ApplicationInstanceInterface, uselist=True, alias='instanceInterfaces')
        ]

    def to_dict(self):
        return {
            'id': self.model.id,
            'name': self.model.name,
            'internalName': self.model.internal_name,
            'type': self.model.type,
            'description': self.model.description,
            **self._resolve_expansions()
        }


class ApplicationInterface(ViewModel):

    model: model.interface.ApplicationInterface

    @staticmethod
    def expansions() -> List[Expansion]:
        return [
            Expansion(key='document', model_cls=Document, uselist=False),
            Expansion(key='application', model_cls=Application, uselist=False),
            Expansion(key='instance_interfaces', model_cls=ApplicationInstanceInterface, uselist=True, alias='instanceInterfaces'),
            Expansion(key='input_value_definitions', model_cls=InputValueDefinition, uselist=True, alias='inputValues'),
            Expansion(key='input_file_definitions', model_cls=InputFileDefinition, uselist=True, alias='inputFiles')
        ]

    def to_dict(self):
        return {
            'id': self.model.id,
            'name': self.model.name,
            'internalName': self.model.internal_name,
            'version': self.model.version,
            **self._resolve_expansions()
        }


class ApplicationInstance(ViewModel):

    model: model.application.ApplicationInstance

    @staticmethod
    def expansions() -> List[Expansion]:
        return [
            Expansion('schedules', model_cls=ApplicationInstanceSchedule, uselist=True),
            Expansion('instance_interfaces', model_cls=ApplicationInstanceInterface, uselist=True, alias='instanceInterfaces'),
            Expansion('heartbeat', model_cls=ApplicationHeartbeat, uselist=False)
        ]

    def to_dict(self):
        return {
            'id': self.model.id,
            'name': self.model.name,
            'internalName': self.model.internal_name,
            **self._resolve_expansions()
        }


class ApplicationHeartbeat(ViewModel):

    model: model.application.Heartbeat

    @staticmethod
    def expansions() -> List[Expansion]:
        return []

    def to_dict(self):
        return {
            'created': self.model.created.isoformat(),
            'updated': self.model.updated.isoformat()
        }


class ApplicationInstanceSchedule(ViewModel):

    model: model.application.ApplicationInstanceSchedule

    @staticmethod
    def expansions():
        return []

    def to_dict(self):
        return {
            'id': self.model.id
        }


class ApplicationInstanceInterface(ViewModel):

    model: model.interface.ApplicationInstanceInterface

    @staticmethod
    def expansions():
        return [
            Expansion('schedules', ApplicationInstanceInterfaceSchedule, uselist=True),
            Expansion('interface', ApplicationInterface, uselist=False),
            Expansion('instance', ApplicationInstance, uselist=False)
        ]

    def to_dict(self):
        return {
            'id': self.model.id,
            **self._resolve_expansions()
        }


class ApplicationInstanceInterfaceSchedule(ViewModel):

    model: model.interface.ApplicationInstanceInterfaceSchedule

    @staticmethod
    def expansions():
        return []

    def to_dict(self):
        return {
            'id': self.model.id,
            'start': self.model.start.strftime('%H:%S'),
            'end': self.model.start.strftime('%H:%S')
        }


class Document(ViewModel):

    model: model.misc.Document

    @staticmethod
    def expansions() -> List[Expansion]:
        return []

    def to_dict(self):
        return {
            'id': self.model.id,
            'type': self.model.type,
            'content': self.model.content
        }


class InputValueDefinition(ViewModel):

    model: model.interface.InputValueDefinition

    @staticmethod
    def expansions() -> List[Expansion]:
        return []

    def to_dict(self):
        return {
            'name': self.model.name,
            'internalName': self.model.internal_name,
            'type': self.model.type,
            'required': self.model.required,
            'default': self.model.default_value,
            'enumerations': list(self.model.allowed_values)
        }


class InputFileDefinition(ViewModel):

    model: model.interface.InputFileDefinition

    @staticmethod
    def expansions() -> List[Expansion]:
        return []

    def to_dict(self):
        return {
            'name': self.model.name,
            'internalName': self.model.internal_name,
            'type': self.model.type,
            'required': self.model.required
        }


class Group(ViewModel):

    model: model.group.Group

    @staticmethod
    def expansions() -> List[Expansion]:
        return []

    def to_dict(self):
        return {
            'id': self.model.id,
            'name': self.model.name
        }


class User(ViewModel):

    model: model.auth.User

    @staticmethod
    def expansions() -> List[Expansion]:
        return []

    def to_dict(self):
        return {
            'id': self.model.id,
            'username': self.model.username
        }


class Job(ViewModel):

    model: model.job.Job

    @staticmethod
    def expansions() -> List[Expansion]:
        return []

    def to_dict(self):
        return {
            'id': self.model.id,
            'name': self.model.name
        }
