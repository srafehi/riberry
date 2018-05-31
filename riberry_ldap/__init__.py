import os

from riberry import plugins, background, model
from riberry.plugins.interfaces import AuthenticationProvider
from ldap3 import Server, Connection, NTLM, SIMPLE


def sample_task():
    print('Hello world!')


class UserData:

    def __init__(self, username, first_name, last_name, display_name, email, department, distinguished_name):
        self.username = username
        self.first_name = first_name
        self.last_name = last_name
        self.display_name = display_name
        self.email = email
        self.department = department[0] if department and isinstance(department, tuple) else (department or None)
        self.distinguished_name = distinguished_name


class GroupData:

    def __init__(self, name, label, description, distinguished_name):
        self.name = name
        self.label = label
        self.description = description
        self.distinguished_name = distinguished_name


class LdapManager:

    def __init__(self, user, password, config):
        self.user = user
        self.password = password
        self.config = config
        self._server = None
        self._connection = None

    @property
    def server(self):
        if self._server is None:
            self._server = Server(self.config['server'], use_ssl=self.config.get('ssl'))
        return self._server

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.make_connection(self.server, self.user, self.password)
        return self._connection

    @staticmethod
    def make_connection(server, user, password, authentication=NTLM):
        conn = Connection(server, user=user, password=password, authentication=authentication)
        if not conn.bind():
            raise Exception('Invalid Credentials')
        return conn

    def authenticate_user(self, username, password):
        user = self.find_user(username)
        assert self.make_connection(self.server, user.distinguished_name, password, SIMPLE).bound
        return user

    def find_user(self, username):

        attributes = [v for v in self.config['user']['attributes']['additional'].values() if v]
        self.connection.search(
            search_base=self.config['user']['searchPath'],
            search_filter=f"(&"
                          f"(objectClass={self.config['user']['class']})"
                          f"({self.config['user']['attributes']['uniqueName']}={username})"
                          f"{self.config['user'].get('extraFilter') or ''}"
                          f")",
            attributes=attributes + [self.config['user']['attributes']['uniqueName'], self.config['user']['attributes']['distinguishedName']]
        )

        results = self.connection.response

        if not results:
            return None

        if len(results) > 1:
            raise Exception('Found multiple users')

        result = results[0]
        dn, user = result['dn'], result['attributes']
        return UserData(
            username=username,
            first_name=self._load_attribute(user, 'user', 'firstName'),
            last_name=self._load_attribute(user, 'user', 'lastName'),
            display_name=self._load_attribute(user, 'user', 'displayName'),
            email=self._load_attribute(user, 'user', 'email'),
            department=self._load_attribute(user, 'user', 'department'),
            distinguished_name=dn,
        )

    def _load_attribute(self, obj, type_, attribute, required=False):
        obj_attribute = self.config[type_]['attributes']['additional'][attribute]
        value = obj[obj_attribute] if obj_attribute else None
        if required and not value:
            raise Exception(f'{attribute!r}/{obj_attribute} is required, though value was None')
        return value

    def find_groups_for_user(self, user: UserData):
        attributes = [v for v in self.config['group']['attributes']['additional'].values() if v]
        self.connection.search(
            search_base=self.config['group']['searchPath'],
            search_filter=f"(&"
                          f"(objectClass={self.config['group']['class']})"
                          f"{self.config['group'].get('extraFilter') or ''}"
                          f"({self.config['group']['attributes']['uniqueName']['membership']}={user.distinguished_name})"
                          f")",
            attributes=attributes + [self.config['group']['attributes']['uniqueName'], self.config['group']['attributes']['distinguishedName']]
        )
        groups = []
        for result in self.connection.response:
            dn, group = result['dn'], result['attributes']
            group_data = GroupData(
                name=group[self.config['group']['attributes']['uniqueName']],
                label=self._load_attribute(group, 'group', 'label'),
                description=self._load_attribute(group, 'group', 'description'),
                distinguished_name=dn,
            )
            groups.append(group_data)

        return groups

    def all_groups(self):
        attributes = [v for v in self.config['group']['attributes']['additional'].values() if v]
        self.connection.search(
            search_base=self.config['group']['searchPath'],
            search_filter=f"(&"
                          f"(objectClass={self.config['group']['class']})"
                          f"{self.config['group'].get('extraFilter') or ''}"
                          f")",
            attributes=attributes + [self.config['group']['attributes']['uniqueName'], self.config['group']['attributes']['distinguishedName']]
        )
        groups = []
        for result in self.connection.response:
            dn, group = result['dn'], result['attributes']
            print(dn, group)
            group_data = GroupData(
                name=group[self.config['group']['attributes']['uniqueName']],
                label=self._load_attribute(group, 'group', 'label'),
                description=self._load_attribute(group, 'group', 'description'),
                distinguished_name=dn,
            )
            groups.append(group_data)

        return groups


class LdapAuthenticationProvider(AuthenticationProvider):

    @classmethod
    def name(cls) -> str:
        return 'ldap'

    def load_manager(self):
        username, password = os.environ[self.raw_config['credentials']['envvar']].split(':', maxsplit=1)
        return LdapManager(user=username, password=password, config=self.raw_config)

    def authenticate(self, username: str, password: str) -> bool:
        manager = self.load_manager()
        user_data = manager.authenticate_user(username=username, password=password)
        user_model = model.auth.User.query().filter_by(username=user_data.username).first()
        if not user_model:
            user_model = model.auth.User(
                username=user_data.username,
                auth_provider=self.name()
            )
            model.conn.add(user_model)

        if not user_model.details:
            user_model.details = model.auth.UserDetails(
                first_name=user_data.first_name,
                last_name=user_data.last_name,
                display_name=user_data.display_name,
                department=user_data.department,
                email=user_data.email
            )
        else:
            user_model.details.first_name = user_data.first_name
            user_model.details.last_name = user_data.last_name
            user_model.details.display_name = user_data.display_name
            user_model.details.department = user_data.department
            user_model.details.email = user_data.email

        model.conn.commit()

        for association in user_model.group_associations:
            model.conn.delete(association)

        groups_data = manager.find_groups_for_user(user=user_data)
        for group_data in groups_data:
            group_model = model.group.Group.query().filter_by(name=group_data.name).first()
            if not group_model:
                group_model = model.group.Group(name=group_data.name)
                model.conn.add(group_model)
            group_association = model.group.ResourceGroupAssociation(
                group=group_model,
                resource_id=user_model.id,
                resource_type=model.group.ResourceType.user
            )
            model.conn.add(group_association)

        model.conn.commit()
        return True

    def secure_password(self, password: bytes) -> bytes:
        raise NotImplementedError

    def on_enabled(self):
        background.register_task('riberry_ldap:sample_task', schedule=5)


plugins.plugin_register['authentication'].add(LdapAuthenticationProvider)
