import riberry


def remove_expired_api_keys():
    """ Removes all expired API keys. """

    riberry.model.auth.UserToken.query().filter(
        riberry.model.base.utc_now() > riberry.model.auth.UserToken.expires
    ).delete()
    riberry.model.conn.commit()
