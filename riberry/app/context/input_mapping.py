import csv
import io
import json
from collections import Mapping
from operator import itemgetter
from typing import Union, AnyStr, Iterator, Any, Dict

import riberry


class InputMappings:

    def __init__(self, context):
        self.values = InputValueMapping(context=context)
        self.files = InputFileMapping(context=context)


class InputMapping(Mapping):

    def __init__(self, context, cls):
        self.context: riberry.app.context.Context = context
        self.cls = cls

    def _get_value(self, instance):
        raise NotImplementedError

    def get(self, item, default=None):
        if isinstance(item, str):
            return super().get(item, default=default)

        return [super(InputMapping, self).get(_, default) for _ in item]

    def __getitem__(self, item: Union[AnyStr, Iterator[AnyStr]]) -> Any:

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

    def __len__(self) -> int:
        return self.cls.query().filter_by(job=self.context.current.job).count()

    def __iter__(self) -> Iterator[AnyStr]:
        instances = riberry.model.conn.query(
            self.cls.internal_name
        ).filter_by(
            job=self.context.current.job
        ).all()

        return map(itemgetter(0), instances)

    @property
    def dict(self) -> Dict[AnyStr, Any]:
        return {
            instance.internal_name: instance.value
            for instance in self.cls.query().filter_by(job=self.context.current.job).all()
        }


class InputValueMapping(InputMapping):

    def __init__(self, context):
        super().__init__(context=context, cls=riberry.model.interface.InputValueInstance)

    def _get_value(self, instance):
        return instance.value


class InputFileMapping(InputMapping):

    def __init__(self, context):
        super().__init__(context=context, cls=riberry.model.interface.InputFileInstance)

    def _get_value(self, instance):
        return InputFileReader(instance)

    def __getitem__(self, item: Union[AnyStr, Iterator[AnyStr]]) -> Union[
        'InputFileReader', Iterator['InputFileReader']]:
        return super().__getitem__(item)


class InputFileReader:
    instance: riberry.model.interface.InputFileInstance

    def __init__(self, file_instance):
        self.instance = file_instance

    @property
    def filename(self):
        return self.instance.filename

    @property
    def size(self):
        return self.instance.size

    def bytes(self) -> bytes:
        return self.instance.binary

    def text(self, *args, **kwargs) -> str:
        return self.bytes().decode(*args, **kwargs)

    def json(self, *args, **kwargs):
        return json.loads(self.bytes(), *args, **kwargs)

    def csv(self, *args, **kwargs):
        reader = csv.DictReader(io.StringIO(self.text()), *args, **kwargs)
        for row in reader:
            yield row

    def __bool__(self):
        return bool(self.instance.binary)
