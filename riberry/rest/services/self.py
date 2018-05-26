from typing import Dict

from riberry import services
from riberry.rest import view_models


def profile(options=None) -> Dict:
    user = services.self.profile()
    return view_models.User(model=user, options=options).to_dict()
