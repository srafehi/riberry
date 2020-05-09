import pytest

import riberry
from riberry.services.misc.reference import Reference, ModelReference


class TestReference:

    @staticmethod
    @pytest.mark.parametrize('url,result', [
        ('riberry:', False),
        ('riberry://', False),
        ('riberry://type', False),
        ('riberry:/type', False),
        ('riberry://type/path', True),
        ('riberry://type/path?id=1', True),
        ('riberry://type/path/to/something', True),
    ])
    def test_is_reference_url(url, result):
        assert Reference.is_reference_url(url) == result

    @staticmethod
    @pytest.mark.parametrize('type,path,properties,result', [
        ('type', 'path', {}, 'riberry://type/path'),
        ('type', pytest, {}, 'riberry://type/pytest'),
        ('type', Reference, {}, 'riberry://type/Reference'),
        ('type', '/path', {}, 'riberry://type/path'),
        ('type', '//path', {}, 'riberry://type/path'),
        ('type', '/path/to/something', {}, 'riberry://type/path/to/something'),
        ('type', 'path', {'id': 1}, 'riberry://type/path?id=1'),
        ('type', 'path', {'id': 1, 'hello': 'world'}, 'riberry://type/path?id=1&hello=world'),
        ('type', 'path', {'id': 1, 'hello': 'world', 'blank': None}, 'riberry://type/path?id=1&hello=world'),
    ])
    def test_url(type, path, properties, result):
        assert Reference(type, path, properties).url == result

    @staticmethod
    @pytest.mark.parametrize('type,path,properties,url', [
        ('type', 'path', {}, 'riberry://type/path'),
        ('type', 'path/to/something', {}, 'riberry://type/path/to/something'),
        ('type', 'path', {'id': '1'}, 'riberry://type/path?id=1'),
        ('type', 'path', {'id': '1', 'hello': 'world'}, 'riberry://type/path?id=1&hello=world'),
        ('type', 'path', {'id': '1', 'hello': 'world'}, 'riberry://type/path?id=1&hello=world&blank='),
    ])
    def test_from_url(type, path, properties, url):
        reference = Reference.from_url(url)
        assert (reference.type, reference.path, reference.properties) == (type, path, properties)


class TestModelReference:

    @staticmethod
    @pytest.mark.parametrize('instance,attribute,result', [
        (riberry.model.job.Job(id=1), 'id', ('model', 'Job', {'id': '1'}, riberry.model.job.Job)),
        (riberry.model.job.Job(id=1), ['id'], ('model', 'Job', {'id': '1'}, riberry.model.job.Job)),
        (riberry.model.job.Job(name='job'), ['name'], ('model', 'Job', {'name': 'job'}, riberry.model.job.Job)),
        (
                riberry.model.job.Job(id=1, name='job'),
                ['id', 'name'],
                ('model', 'Job', {'id': '1', 'name': 'job'}, riberry.model.job.Job),
        ),
    ])
    def test_from_instance(instance, attribute, result):
        reference = ModelReference.from_instance(instance=instance, attribute=attribute)
        assert (
            reference.type,
            reference.path,
            reference.properties,
            reference.model,
        ) == result

    @staticmethod
    @pytest.mark.parametrize('url,result', [
        ('riberry://model/Job', True),
        ('riberry://model/Job?id=123', True),
        ('rib://model/Job', False),
        ('riberry://object/Job', False),
        ('riberry://model/NonExistentModel', False),
    ])
    def test_is_reference_url(url, result):
        assert ModelReference.is_reference_url(url) == result

    @staticmethod
    @pytest.mark.parametrize('model,result', [
        ('Job', True),
        (riberry.model.interface.InputFileInstance, True),
        (Reference, False),
    ])
    def test_is_model(model, result):
        assert ModelReference.is_model(model) == result

    @staticmethod
    @pytest.mark.parametrize('model,result', [
        ('Job', riberry.model.job.Job),
        (riberry.model.job.Job, riberry.model.job.Job),
        (riberry.model.job.Job(), riberry.model.job.Job),
    ])
    def test_model(model, result):
        assert ModelReference(model).model == result

    def test_query(self):
        reference = ModelReference('Job', properties={'name': 'job'})
        actual_query = reference.query().statement.compile(compile_kwargs={"literal_binds": True})
        expected_query = riberry.model.job.Job.query().filter_by(
            name='job'
        ).statement.compile(compile_kwargs={"literal_binds": True})

        assert str(actual_query) == str(expected_query)

    @staticmethod
    @pytest.mark.parametrize('model,result', [
        ('Job', riberry.model.job.Job),
        ('InputFileInstance', riberry.model.interface.InputFileInstance),
        (riberry.model.interface.InputFileInstance, riberry.model.interface.InputFileInstance),
        (riberry.model.interface.InputFileInstance(), riberry.model.interface.InputFileInstance),
        (Reference, ValueError),
        (int, ValueError),
        (riberry, ValueError),
    ])
    def test_resolve_model(model, result):
        if issubclass(result, Exception):
            with pytest.raises(result):
                ModelReference.resolve_model(model)
        else:
            assert ModelReference.resolve_model(model) == result
