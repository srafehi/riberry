import riberry


class Addon:

    def register(self, riberry_app: 'riberry.app.base.RiberryApplication'):
        raise NotImplementedError
