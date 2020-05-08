import json
import os
import uuid
from itertools import chain
from typing import List, Dict

import yaml
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.exc import NoResultFound

from riberry import model, services, policy
from riberry.typing import ModelType
from riberry.util.common import variable_substitution


class Loader(yaml.SafeLoader):
    """ https://stackoverflow.com/a/9577670 """

    def __init__(self, stream):
        self._root = os.path.split(stream.name)[0]
        super(Loader, self).__init__(stream)

    def include(self, node):
        filename = variable_substitution(os.path.join(self._root, self.construct_scalar(node)))

        with open(filename, 'r') as f:
            if filename.endswith('.yaml') or filename.endswith('.yml'):
                return yaml.load(f, Loader)
            else:
                return f.read()


Loader.add_constructor('!include', Loader.include)


def collection_diff(obj, collection_name, loader):
    current_collection = set(getattr(obj, collection_name))
    new_collection = set(loader())
    for stale in current_collection - new_collection:
        model.conn.delete(stale)
    setattr(obj, collection_name, list(new_collection))
    return obj


def model_diff(obj):
    object_info = inspect(obj)
    return {
        name: attr.history for name, attr in object_info.attrs.items()
        if attr.history.has_changes()
    }


def session_diff():
    diff = {}
    for obj in sorted(model.conn.dirty | model.conn.new | model.conn.deleted, key=lambda o: str(o)):
        type_ = 'Modified' if obj in model.conn.dirty else 'Added' if obj in model.conn.new else 'Deleted' if obj in model.conn.deleted else 'REF'
        diff[obj] = type_, model_diff(obj)
    return diff


def import_applications(applications, restrict=None):
    existing_apps = {a.internal_name: a for a in model.application.Application.query().all()}

    if not restrict:
        for stale in set(existing_apps) - set(applications):
            model.conn.delete(existing_apps[stale])

    apps = []
    for application, properties in applications.items():
        if restrict and application not in restrict:
            continue
        app = import_application(internal_name=application, attributes=properties)
        apps.append(app)
    return apps


def import_application(internal_name, attributes):
    try:
        app = services.application.application_by_internal_name(internal_name=internal_name)
        app = services.application.update_application(app, attributes)
    except NoResultFound:
        app = services.application.create_application(
            internal_name=internal_name,
            name=attributes.get('name'),
            description=attributes.get('description'),
            type=attributes.get('type'),
            document=None
        )

    if attributes.get('document'):
        if not app.document:
            app.document = model.misc.Document()
            model.conn.add(app.document)
        app.document.content = attributes['document'].encode()
    else:
        if app.document:
            app.document = None

    import_instances(app, attributes.get('instances') or {})
    import_forms(app, attributes.get('forms') or {})

    return app


def import_instances(app, instances):
    return collection_diff(
        obj=app,
        collection_name='instances',
        loader=lambda: {
            import_instance(app, name, attrs)
            for name, attrs in instances.items()
        }
    )


def import_forms(app, forms):
    return collection_diff(
        obj=app,
        collection_name='forms',
        loader=lambda: {
            import_form(app, name, attrs)
            for name, attrs in forms.items()
        }
    )


def import_form(app, internal_name, attributes):
    instance_internal_name = attributes['instance']
    instances = [instance for instance in app.instances if instance.internal_name == instance_internal_name]
    if not instances:
        raise ValueError(f'Could not find instance {instance_internal_name} for form {internal_name}')
    instance = instances[0]

    try:
        form = services.form.form_by_internal_name(internal_name=internal_name)
        form = services.form.update_form(form, attributes)
        if form.instance != instance.id:
            form.instance = instance

    except NoResultFound:
        form = services.form.create_form(
            application=app,
            instance=instance,
            name=attributes.get('name'),
            internal_name=internal_name,
            version=attributes.get('version'),
            description=attributes.get('description'),
            input_files=[],
            input_values=[]
        )

    if attributes.get('document'):
        if not form.document:
            form.document = model.misc.Document()
            model.conn.add(form.document)
        form.document.content = attributes['document'].encode()
    else:
        if form.document:
            form.document = None

    import_form_inputs(
        form=form,
        input_files=attributes.get('inputFiles') or {},
        input_values=attributes.get('inputValues') or {},
    )

    return form


def import_input_file_definition(form, internal_name, attributes):
    try:
        if not form.id:
            raise NoResultFound
        definition = services.form.file_definition_by_internal_name(form=form, internal_name=internal_name)
        definition = services.form.update_file_definition(definition, attributes)
    except NoResultFound:
        definition = model.interface.InputFileDefinition(internal_name=internal_name, **attributes)
        model.conn.add(definition)

    return definition


def import_input_value_definition(form, internal_name, attributes):
    mapping = {
        'enumerations': ('allowed_binaries', lambda values: [json.dumps(v).encode() for v in values]),
        'default': ('default_binary', lambda v: json.dumps(v).encode()),
    }

    attributes = dict(
        (mapping[k][0], mapping[k][1](v)) if k in mapping else (k, v)
        for k, v in attributes.items()
    )

    try:
        if not form.id:
            raise NoResultFound
        definition = services.form.value_definition_by_internal_name(form=form, internal_name=internal_name)
        definition = services.form.update_value_definition(definition, attributes)
    except NoResultFound:
        definition = model.interface.InputValueDefinition(internal_name=internal_name, **attributes)
        model.conn.add(definition)

    return definition


def import_form_inputs(form, input_files, input_values):
    collection_diff(
        obj=form,
        collection_name='input_file_definitions',
        loader=lambda: {
            import_input_file_definition(form, name, attrs)
            for name, attrs in input_files.items()
        }
    )

    collection_diff(
        obj=form,
        collection_name='input_value_definitions',
        loader=lambda: {
            import_input_value_definition(form, name, attrs)
            for name, attrs in input_values.items()
        }
    )


def import_instance(app, internal_name, attributes):
    try:
        instance = services.application_instance.application_instance_by_internal_name(internal_name=internal_name)
        instance = services.application_instance.update_application_instance(instance, attributes)
    except NoResultFound:
        instance = services.application_instance.create_application_instance(
            application=app,
            internal_name=internal_name,
            name=attributes.get('name'),
            schedules=[]
        )

    current_schedules = {}
    for schedule in instance.schedules:
        current_schedules[(
            schedule.days,
            schedule.start_time,
            schedule.end_time,
            schedule.timezone,
            schedule.parameter,
            schedule.value,
            schedule.priority,
        )] = schedule

    loaded_schedules = set((
        sched['days'].lower() if isinstance(sched.get('days'), str) else sched.get('days', '*'),
        model.application.ApplicationInstanceSchedule.cleanse_time(
            sched.get('startTime') or model.application.ApplicationInstanceSchedule.start_time.default.arg),
        model.application.ApplicationInstanceSchedule.cleanse_time(
            sched.get('endTime') or model.application.ApplicationInstanceSchedule.end_time.default.arg),
        sched.get('timeZone') or model.application.ApplicationInstanceSchedule.timezone.default.arg,
        sched['parameter'],
        str(sched['value']) if sched['value'] not in ('', None) else None,
        sched.get('priority') or model.application.ApplicationInstanceSchedule.priority.default.arg,
    ) for sched in attributes.get('schedules', []))

    for stale in set(current_schedules) - loaded_schedules:
        model.conn.delete(current_schedules[stale])

    new_schedules = []
    for schedule in loaded_schedules:
        if schedule in current_schedules:
            continue
        days, start_time, end_time, timezone, parameter, value, priority = schedule
        new_schedules.append(
            dict(
                days=days,
                start_time=start_time,
                end_time=end_time,
                timezone=timezone,
                parameter=parameter,
                value=value,
                priority=priority,
            )
        )

    instance.schedules += services.application_instance.create_application_instance_schedules(new_schedules)
    return instance


def merge_permissions(
    group: model.group.Group,
    permission_names: List[str],
):
    unprocessed_permissions = {permission.name: permission for permission in group.permissions}
    for permission_name in permission_names:
        if permission_name in unprocessed_permissions:
            unprocessed_permissions.pop(permission_name)
        else:
            permission = model.group.GroupPermission(name=permission_name, group=group)
            model.conn.add(permission)
    stale_permissions = list(unprocessed_permissions.values())
    return stale_permissions


def merge_associations(
    group: model.group.Group,
    resource_ids: List[int],
    resource_type: List[model.misc.ResourceType],
    model_type: ModelType,
):
    stale_instances = {
        model_type.get(association.resource_id).id: association
        for association in group.resource_associations
        if association.resource_type == resource_type
    }

    for resource_id in resource_ids:
        if resource_id in stale_instances:
            stale_instances.pop(resource_id)
        else:
            association = model.group.ResourceGroupAssociation(
                resource_id=resource_id,
                resource_type=resource_type,
                group=group,
            )
            model.conn.add(association)

    return list(stale_instances.values())


def import_groups(group_definitions: Dict[str, Dict]):

    def get_ids(model_type: ModelType, internal_names: List[str]) -> List[int]:
        return [
            instance.id
            for instance in model_type.query().filter(model_type.internal_name.in_(internal_names)).all()
        ]

    group_definitions = {
        group_name: {
            'name': group_name,
            'display_name': group_definition.get('name'),
            'description': group_definition.get('description'),
            'permissions': group_definition.get('permissions') or [],
            'applications': get_ids(model.application.Application, group_definition.get('applications') or []),
            'forms': get_ids(model.interface.Form, group_definition.get('forms') or []),
        }
        for group_name, group_definition in group_definitions.items()
    }

    groups = {group.name: group for group in model.group.Group.query().all()}
    for group_name, definition in group_definitions.items():
        if group_name not in groups:
            groups[group_name] = services.auth.create_group(name=group_name)

        group: model.group.Group = groups.pop(group_name)
        group.display_name = definition.get('display_name')
        group.description = definition.get('description')
        model.conn.add(group)

        stale_permissions = merge_permissions(
            group=group,
            permission_names=definition['permissions'],
        )

        stale_applications = merge_associations(
            group=group,
            resource_ids=definition['applications'],
            resource_type=model.misc.ResourceType.application,
            model_type=model.application.Application,
        )

        stale_forms = merge_associations(
            group=group,
            resource_ids=definition['forms'],
            resource_type=model.misc.ResourceType.form,
            model_type=model.interface.Form,
        )

        for association in chain(stale_applications, stale_forms, stale_permissions):
            model.conn.delete(association)
        if not group.resource_associations and not group.permissions:
            model.conn.delete(group)


def _convert_value(value):
    if isinstance(value, list):
        return [_convert_value(v) for v in value]
    if isinstance(value, model.base.Base):
        return dict(
            name=type(value).__name__,
            id=getattr(value, 'id', None)
        )
    if isinstance(value, str) and len(value) > 512:
        return f'(trimmed) {value[:512]}'
    if isinstance(value, bytes):
        return f'bytes (size: {len(value)})'
    import enum
    if isinstance(value, enum.Enum):
        return value.name
    return value


def json_diff(diff):
    output = {'Modified': [], 'Added': [], 'Deleted': []}

    for obj, (diff_type, changes) in diff.items():
        if not changes and diff_type not in ('Added', 'Deleted'):
            continue
        entry = {}
        for k, v in changes.items():
            attr = {}
            if v.deleted:
                attr['Deleted'] = _convert_value(v.deleted[0])
            if v.added and v.added[0]:
                attr['Added'] = _convert_value(v.added[0])
            entry[k] = attr
        obj_dict = _convert_value(obj)
        output[diff_type].append({
            'id': obj_dict['id'],
            'type': obj_dict['name'],
            'attributes': entry
        })

    return output


def import_menu_for_forms(menu):
    for item in model.misc.MenuItem.query().all():
        model.conn.delete(item)

    for item in menu:
        menu_item = import_menu_item(item, menu_type='forms', parent=None)
        model.conn.add(menu_item)


def import_menu_item(item, menu_type, parent=None):
    menu_item = model.misc.MenuItem(parent=parent, menu_type=menu_type)
    if isinstance(item, dict):
        menu_item.key = str(uuid.uuid4())
        menu_item.type = 'branch'
        menu_item.label = list(item.keys())[0]
        menu_item.children = [
            import_menu_item(child, menu_type, menu_item)
            for child in list(item.values())[0]
        ]
    elif isinstance(item, str):
        menu_item.key = item
        menu_item.type = 'leaf'
    return menu_item


def import_config(config, formatter=None, restrict_apps=None):
    applications = config.get('applications') or {}
    import_applications(applications=applications, restrict=restrict_apps)
    if not restrict_apps:
        import_capacities(config.get('capacity-configuration') or {})

    diff = session_diff()
    model.conn.flush()

    import_menu_for_forms(menu=config.get('menues', {}).get('forms', {}))
    import_groups(group_definitions=config.get('groups', {}))

    for k, v in session_diff().items():
        if k in diff:
            diff[k] = diff[k][0], {**diff[k][1], **v[1]}
        else:
            diff[k] = v

    model.conn.flush()
    return formatter(diff) if formatter else diff


def import_capacities(capacities):
    existing = {c.weight_parameter: c for c in model.application.CapacityConfiguration.query().all()}

    for stale in set(existing) - set(capacities):
        model.conn.delete(existing[stale])

    for weight_name, properties in capacities.items():
        import_capacity(weight_name, properties)


def import_capacity(weight_parameter, properties):
    capacity_config: model.application.CapacityConfiguration = \
        model.application.CapacityConfiguration.query().filter_by(weight_parameter=weight_parameter).first()

    if not capacity_config:
        capacity_config = model.application.CapacityConfiguration(weight_parameter=weight_parameter)

    capacity_config.capacity_parameter = properties['parameters']['capacity']
    capacity_config.producer_parameter = properties['parameters']['producer']

    capacity_config.distribution_strategy = (
        model.application.CapacityDistributionStrategy(properties['strategy'])
        if 'strategy' in properties
        else model.application.CapacityConfiguration.distribution_strategy.default.arg
    )

    new_producers = {p['internalName']: p for p in properties['producers']}

    # Update existing, delete old
    for producer in capacity_config.producers:
        if producer.internal_name not in new_producers:
            model.conn.delete(producer)
        else:
            producer_config = new_producers.pop(producer.internal_name)
            producer.name = producer_config.get('name') or producer.internal_name
            producer.capacity = producer_config['capacity']

    # Add new
    for internal_name, producer_config in new_producers.items():
        capacity_config.producers.append(
            model.application.CapacityProducer(
                internal_name=internal_name,
                name=producer_config.get('name') or internal_name,
                capacity=producer_config['capacity'],
            )
        )

    model.conn.add(capacity_config)


def import_from_file(config_path, dry_run=True, formatter=json_diff, restrict_apps=None):
    with open(config_path) as f:
        config = variable_substitution(yaml.load(f, Loader) or {})

    with model.conn.no_autoflush, policy.context.disabled_scope():
        output = import_config(config, formatter=formatter, restrict_apps=restrict_apps)

    if dry_run:
        model.conn.rollback()
    else:
        model.conn.commit()

    return output
