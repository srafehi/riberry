from functools import lru_cache
from inspect import isclass
from typing import Any, Optional, Dict, List, Union, Type
from urllib.parse import ParseResult, parse_qsl, urlencode, urlparse

import riberry
from sqlalchemy.orm import Query


class Reference:
    """ Builds and generates Riberry reference URLs.

    URLs will be returned in the form riberry://<type>/<path>?<properties>

    Example: "riberry://operation/git_branches?repo=http://..."
    """

    scheme = 'riberry'

    def __init__(
            self,
            type: str,
            path: Any,
            properties: Optional[Dict] = None,
    ):
        self.type: str = type
        self.path: str = _cleanse_path(_make_path(path))
        self.properties: dict = properties or {}

    @property
    def url(self) -> str:
        """ Returns the generated URL for the given reference. """

        # noinspection PyArgumentList
        return ParseResult(
            scheme=self.scheme,
            netloc=self.type,
            path=_make_path(obj=self.path),
            params='',
            query=urlencode(_cleanse_properties(self.properties) or {}),
            fragment='',
        ).geturl()

    @classmethod
    def from_url(cls, url: str) -> 'Reference':
        """ Converts the given url string into a Reference instance. """

        if cls.is_reference_url(url):
            return cls._from_parse_result(_parse_url(url))
        else:
            raise ValueError(f'{cls.__name__}.from_url :: invalid URL supplied: {url}')

    @classmethod
    def is_reference_url(cls, url: str) -> bool:
        """ Checks to see if the given url is a Riberry reference url. """

        if not isinstance(url, str):
            return False
        result = _parse_url(url)
        return result.scheme == cls.scheme and bool(result.netloc) and bool(result.path)

    @classmethod
    def _from_parse_result(cls, result: ParseResult) -> 'Reference':
        """ Creates a Reference instance from a parsed URL. """

        return cls(type=result.netloc, path=result.path, properties=_parse_query(result.query))


class ModelReference(Reference):
    """ Builds and generates Riberry reference URLs of type "model".

    URLs will be returned in the form riberry://model/<model>?<properties>

    Example: "riberry://model/Form?internal_name=some_name"
    """

    type = 'model'

    def __init__(
            self,
            model,
            properties: Optional[Dict] = None,
    ):
        assert self.is_model(model=model)
        super().__init__(type=ModelReference.type, path=model, properties=properties)

    @property
    def model(self) -> riberry.model.base.Base:
        """ Returns the SQLAlchemy model for the current reference. """

        return self.resolve_model(self.path.lstrip('/'))

    def query(self) -> Query:
        """ Returns an SQLAlchemy query object for the reference's model with the defined properties populated. """

        return self.model.query().filter_by(**self.properties)

    @classmethod
    def from_instance(cls, instance, attribute: Union[str, List[str]] = 'id') -> 'ModelReference':
        """ Created a model reference from an instance and its attributes. """

        query = {}
        query_attributes = attribute if isinstance(attribute, list) else [attribute]
        for attribute in query_attributes:
            value = getattr(instance, attribute)
            query[attribute] = str(value) if value is not None else None

        return cls(model=type(instance), properties=query)

    @classmethod
    def is_model(cls, model: Union[riberry.model.base.Base, Type[riberry.model.base.Base], str]) -> bool:
        """ Checks to see if the given instance/type/string is a Riberry SQLAlchemy model. """

        try:
            return bool(cls.resolve_model(model))
        except ValueError:
            return False

    @classmethod
    def is_reference_url(cls, url) -> bool:
        """ Checks to see if the given url is a Riberry model reference url. """

        if not super().is_reference_url(url):
            return False

        result = _parse_url(url)
        if result.netloc != cls.type:
            return False

        model = result.path.lstrip('/')
        if not cls.is_model(model):
            return False

        return True

    @classmethod
    @lru_cache()
    def resolve_model(
            cls,
            model: Union[riberry.model.base.Base, Type[riberry.model.base.Base], str],
    ) -> riberry.model.base.Base:
        """ Returns the Riberry SQLALchemy model class for the given input. """

        if isinstance(model, riberry.model.base.Base):
            return type(model)
        elif isclass(model) and issubclass(model, riberry.model.base.Base):
            return model
        try:
            # noinspection PyProtectedMember,PyUnresolvedReferences
            return riberry.model.base.Base._decl_class_registry[model]
        except KeyError:
            raise ValueError(f'{cls.__name__}.resolve_model :: could not resolve model {model}')

    @classmethod
    def _from_parse_result(cls, result: ParseResult) -> 'ModelReference':
        """ Creates a Riberry model reference instance from a parsed URL. """

        return cls(
            model=result.path,
            properties=_parse_query(result.query),
        )


def _cleanse_path(path: str) -> str:
    """ Strips out the leading forward slash from the path. """

    return path.lstrip('/') if path else ''


def _cleanse_properties(properties: dict) -> dict:
    """ Remove any None values. """

    return {key: value for key, value in properties.items() if value is not None}


def _make_path(obj: Any) -> str:
    """ Converts the given object into a string, suitable for a path component. """

    if isinstance(obj, str):
        return obj
    elif hasattr(obj, '__name__'):
        return obj.__name__
    else:
        return type(obj).__name__


def _parse_query(query: str) -> Dict[str, str]:
    """ Returns a mapping for the given query string. """

    return dict(parse_qsl(query or ''))


@lru_cache()
def _parse_url(url: str) -> ParseResult:
    """ Returns an object containing the components of the parsed url. """

    result = urlparse(url)
    # noinspection PyArgumentList
    return ParseResult(
        scheme=result.scheme,
        netloc=result.netloc,
        path=_cleanse_path(path=result.path),
        params=result.params,
        query=result.query,
        fragment=result.fragment,
    )
