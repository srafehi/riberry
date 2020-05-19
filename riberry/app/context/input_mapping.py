import abc
import csv
import io
import json
from collections import Mapping
from operator import itemgetter
from typing import Union, AnyStr, Iterator, Any, Dict

import riberry
from riberry.services.misc.reference import ModelReference


class InputMappings:
    """ Exposes helpers to read values and files associated to an active execution context. """

    def __init__(self, context):
        self.values: InputValueMapping = InputValueMapping(context=context)
        self.files: InputFileMapping = InputFileMapping(context=context)

    @property
    def data(self) -> Any:
        """ Returns the input value for job's InputDefinition. """

        return self.values.get(riberry.services.job.JobBuilder.input_value_instance_key)


class InputMapping(Mapping, metaclass=abc.ABCMeta):
    """ Base class for reading a job's inputs based on internal_names. """

    def __init__(self, context, cls: riberry.model.base.Base):
        self.context: riberry.app.context.Context = context
        self.cls = cls

    def __getitem__(self, item: Union[AnyStr, Iterator[AnyStr]]) -> Any:
        """ Retrieves one or more inputs. """

        query = self.cls.query().filter(self.cls.job == self.context.current.job)

        if isinstance(item, str):
            instance = query.filter_by(internal_name=item).first()
            if not instance:
                raise KeyError(item)
            return self._get_value(instance)
        else:
            instances = query.filter(self.cls.internal_name.in_(item)).all()
            mapping = {instance.internal_name: self._get_value(instance) for instance in instances}
            return tuple(mapping[key] for key in item)

    def __iter__(self) -> Iterator[AnyStr]:
        """ Retrieves all input "internal_name" values. """

        instances = riberry.model.conn.query(
            self.cls.internal_name
        ).filter_by(
            job=self.context.current.job
        ).all()

        return map(itemgetter(0), instances)

    def __len__(self) -> int:
        """ Retrieves the count of input instances for the current job. """

        return self.cls.query().filter_by(job=self.context.current.job).count()

    @property
    def dict(self) -> Dict[AnyStr, Any]:
        """ Retrieves a mapping for all input instances for the current job. """

        return {
            instance.internal_name: self._get_value(instance)
            for instance in self.cls.query().filter_by(job=self.context.current.job).all()
        }

    def get(self, item, default=None):
        """ Extension to Mapping.get which supports multiple keys as input. """

        if isinstance(item, str):
            return super().get(item, default=default)

        return [super(InputMapping, self).get(_, default) for _ in item]

    @abc.abstractmethod
    def _get_value(self, instance):
        """ Retrieves the value of the current input instance. """

        raise NotImplementedError


class InputFileReader:
    """ Provides helper functions for reading InputFileInstance instances. """

    def __init__(self, file_instance: riberry.model.interface.InputFileInstance):
        self.instance: riberry.model.interface.InputFileInstance = file_instance

    def __bool__(self):
        """ True if the binary is not empty. """

        return bool(self.instance.binary)

    @property
    def filename(self) -> str:
        """ Returns the filename of the given file. """

        return self.instance.filename

    @property
    def size(self) -> int:
        """ Returns the size of the file in bytes. """

        return self.instance.size

    def bytes(self) -> bytes:
        """ Returns the file as a byte string. """

        return self.instance.binary

    def csv(self, *args, **kwargs) -> csv.DictReader:
        """ Returns the file as a csv DictReader. """

        reader = csv.DictReader(io.StringIO(self.text()), *args, **kwargs)
        for row in reader:
            yield row

    def json(self, *args, **kwargs):
        """ Returns the file as a de-serialized JSON object. """

        return json.loads(self.bytes(), *args, **kwargs)

    def text(self, *args, **kwargs) -> str:
        """ Returns the file as a string. """

        return self.bytes().decode(*args, **kwargs)


class InputValueMapping(InputMapping):
    """ Provides helper functions for reading InputValueInstance instances. """

    def __init__(self, context):
        super().__init__(context=context, cls=riberry.model.interface.InputValueInstance)

    def _get_value(self, instance) -> Any:
        """ Returns the value from the given instance. """
        return instance.value


class InputFileMapping(InputMapping):
    """ Helps to extract input files for a job associated with the given context. """

    def __init__(self, context):
        super().__init__(context=context, cls=riberry.model.interface.InputFileInstance)

    def __getitem__(
            self,
            item: Union[AnyStr, Iterator[AnyStr]],
    ) -> Union['InputFileReader', Iterator['InputFileReader']]:
        """ Returns a InputFileReader instance for each given InputFileInstance.internal_name """

        return super().__getitem__(item)

    def dereference(self, url: str) -> 'InputFileReader':
        """ De-references a InputFileInstance model reference url. """

        if ModelReference.is_reference_url(url):
            ref = ModelReference.from_url(url)
            if ref.model == self.cls and 'internal_name' in ref.properties:
                return self[ref.properties['internal_name']]
        raise ValueError(
            f'InputFileMapping.dereference:: Expected model reference url with internal_name property, got {url}'
        )

    def _get_value(self, instance) -> InputFileReader:
        """ Creates a InputFileReader for the given InputFileInstance. """

        return InputFileReader(instance)
