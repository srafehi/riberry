

class RiberryApplicationBackend:

    def __init__(self, instance):
        self.instance = instance

    def register_task(self, func, **kwargs):
        raise NotImplementedError

    def task_by_name(self, name: str):
        raise NotImplementedError

    def start_execution(self, execution_id, root_id, entry_point) -> str:
        raise NotImplementedError

    def create_receiver_task(self, external_task_id, validator):
        raise NotImplementedError

    def active_task(self):
        raise NotImplementedError

    def __getattr__(self, item):
        return getattr(self.instance, item)
