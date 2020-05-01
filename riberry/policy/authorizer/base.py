import functools
from typing import Iterable

from sqlalchemy.orm import Query

from riberry.model.auth import User
from riberry.typing import ModelType


class QueryAuthorizerContext:

    def __init__(self, subject: User, requested_permission):
        self.subject: User = subject
        self.permissions = subject.permissions_to_domain_ids()
        self.traversed = set()
        self.requested_permission = requested_permission

    @staticmethod
    def unique_join(query: Query, model: ModelType) -> Query:
        if model in (c.entity for c in getattr(query, '_join_entities')):
            return query
        return query.join(model)


class StepResult:

    def __init__(self, query: Query, next_model: ModelType = None, expression=None):
        self.query: Query = query
        self.next_model: ModelType = next_model
        self.expression = expression


class PermissionDomainQueryAuthorizer:

    def __init__(self):
        self.step_resolvers = {}

    def _find_starting_model(
            self,
            query: Query,
            models: Iterable[ModelType],
    ) -> ModelType:
        if not models:
            assert len(query.column_descriptions) == 1
            return query.column_descriptions[0]['entity']
        elif isinstance(models, Iterable):
            for model in models:
                if model in self.step_resolvers:
                    return model
        else:
            raise ValueError(f'Could not derive starting model from {", ".join(m.__name__ for m in models)}')

    def apply_filter(self, query: Query, *source_models, context=None) -> Query:
        expression = None
        current_model = self._find_starting_model(query=query, models=source_models)
        while current_model:
            func = self.step_resolvers[current_model]
            result: StepResult = func(query=query, context=context)
            context.traversed.add(current_model)
            query = result.query
            current_model = result.next_model
            assert current_model not in context.traversed, f'Encountered circular traversal to {current_model}'
            if result.expression is not None:
                expression = (expression | result.expression) if expression is not None else result.expression

        return query, expression

    def register_chain(self, *models):
        for source, target in zip(models, models[1:]):
            self.register_step(source, target)

    def register_resolver(self, model):
        def inner(func):
            self._add_step_resolver(model, func)
            return func

        return inner

    def register_step(self, source, target):
        self._add_step_resolver(
            source,
            functools.partial(self._resolve, model=target),
        )

    def _add_step_resolver(self, source, func):
        assert source not in self.step_resolvers
        self.step_resolvers[source] = func

    @staticmethod
    def _resolve(query, model, context) -> StepResult:
        return StepResult(context.unique_join(query, model), model)
