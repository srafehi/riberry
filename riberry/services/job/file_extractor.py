import hashlib
from typing import Any, Tuple, List, Optional

from riberry.model.helpers import max_string_length
from riberry.model.interface import InputFileInstance
from riberry.services.misc.data_uri import DataUri
from riberry.services.misc.reference import ModelReference


class InputFileExtractor:
    """ Replaces data-uri strings with InputFileInstance references. """

    def __init__(self, input_data: Any):
        self.input_data: Any = input_data

    def extract(self, root: Optional[str] = None) -> Tuple[Any, List[InputFileInstance]]:
        """ Returns converted input data and extracted InputFileInstance instances. """

        files: List[InputFileInstance] = []
        input_data = self.extract_from_value(self.input_data, files=files, parent=root)
        return input_data, files

    @classmethod
    def extract_from_value(
            cls,
            input_data: Any,
            files: List[InputFileInstance],
            parent: Optional[str] = None,
    ):
        """ Recursively extracts any data-uris and stores them as InputFileInstance instances
        in the provided `files` list.
        """

        # iterate over all values of dictionaries
        if isinstance(input_data, dict):
            return {
                key: cls.extract_from_value(value, files, cls._extend_key(parent, key))
                for key, value in input_data.items()
            }

        # iterate over all items in lists/tuples/sets
        elif isinstance(input_data, (tuple, list, set)):
            input_data_type = type(input_data)
            return input_data_type(
                cls.extract_from_value(value, files, cls._extend_key(parent, idx))
                for idx, value in enumerate(input_data)
            )

        # create an InputFileInstance for data-uris + replace it with its ModelReference url
        elif isinstance(input_data, str) and DataUri.is_data_uri(value=input_data):
            internal_name = cls._clip_internal_name(parent)
            data_uri = DataUri(input_data)
            input_file_instance = InputFileInstance(
                name=internal_name,
                internal_name=internal_name,
                filename=data_uri.properties.get('name') or internal_name,
                size=data_uri.size,
                binary=data_uri.binary,
            )
            files.append(input_file_instance)
            return ModelReference.from_instance(input_file_instance, attribute='internal_name').url

        # no additional processing required for this type - return without modification
        else:
            return input_data

    @staticmethod
    def _clip_internal_name(internal_name):
        """ If the property exceeds the length of its column, clip it and append a hash to ensure uniqueness. """

        column_length = max_string_length(InputFileInstance.internal_name)
        if len(internal_name) > column_length:
            hash_string = hashlib.md5(internal_name.encode()).hexdigest()[:4]
            internal_name = f'{internal_name[:column_length - 5]}.{hash_string}'
        return internal_name

    @staticmethod
    def _extend_key(parent_key, child_key):
        """ Extends the given key by appending the child key to the parent key. """

        return f'{parent_key}.{child_key}' if parent_key else str(child_key)
