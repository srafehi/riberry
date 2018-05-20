from .engine import Rule, AttributeContext, Policy


class ShorthandRule(Rule):

    def __init__(self, func):
        self.func = func

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        return self.func(context)


class ShorthandPolicy(Policy):

    def __init__(self, func, *collection):
        super(ShorthandPolicy, self).__init__(*collection)
        self.func = func

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        return self.func(context)


class ShorthandPolicySet(Policy):

    def __init__(self, func, *collection):
        super(ShorthandPolicySet, self).__init__(*collection)
        self.func = func

    def target_clause(self, context: AttributeContext) -> bool:
        return True

    def condition(self, context: AttributeContext) -> bool:
        return self.func(context)


def rule(func):
    return ShorthandRule(func)


def policy(func):

    def builder(*collection):
        return ShorthandPolicy(func, *collection)

    return builder


def policy_set(func):

    def builder(*collection):
        return ShorthandPolicySet(func, *collection)

    return builder