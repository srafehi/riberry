import functools
from typing import Iterable, Tuple, Set, Dict, Optional, List

from sqlalchemy.orm import Query

from riberry.model.auth import User
from riberry.typing import ModelType, Model


class Node:

    def __init__(self, model: ModelType, dependents: Tuple['Node'] = ()):
        self.model: ModelType = model
        self.dependents: Tuple[Node] = tuple(dependents)


class QueryAuthorizerContext:

    def __init__(
            self,
            subject: User,
            requested_permission: str,
            requested_operation: str,
            source_model: Optional[ModelType] = None,
            target_entities: Optional[List[Model]] = None,
    ):
        self.subject: User = subject
        self.permissions: Dict[str, Set[str]] = subject.permissions_to_domain_ids()
        self.traversed = set()
        self.requested_permission: str = requested_permission
        self.requested_operation: str = requested_operation
        self.source_model: Optional[ModelType] = source_model
        self.target_entities: Optional[List[Model]] = target_entities

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
            for desc in query.column_descriptions:
                if desc['entity'] is not None:
                    return desc['entity']
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

    def register_resolver(self, model: ModelType):
        def inner(func):
            self._add_step_resolver(model, func)
            return func

        return inner

    def register_step(self, source: ModelType, target: ModelType):
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

    def register_node(self, node: Node):
        nodes = [node]
        while nodes:
            current_node: Node = nodes.pop(0)
            for dependent in current_node.dependents:
                self.register_step(source=dependent.model, target=current_node.model)
                nodes.append(dependent)
