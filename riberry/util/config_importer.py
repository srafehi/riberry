import json
import os

import yaml
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.exc import NoResultFound

from riberry import model, services, policy
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
    import_interfaces(app, attributes.get('interfaces') or {})
    import_forms(app, attributes.get('forms') or [])

    return app


def import_forms(app: model.application.Application, forms):
    existing_forms = {
        (form.instance.internal_name, form.interface.internal_name, form.interface.version): form
        for form in app.forms
    }
    instance_mapping = {i.internal_name: i for i in app.instances}
    interface_mapping = {(i.internal_name, i.version): i for i in app.interfaces}
    new_forms = []

    for form in forms:
        key = instance_name, interface_name, interface_version = (
            form['instance'], form['interface']['internalName'], form['interface']['version']
        )

        if instance_name not in instance_mapping:
            raise Exception(f'Instance not found for form {key}')

        if (interface_name, interface_version) not in interface_mapping:
            raise Exception(f'Interface not found for form {key}')

        if key in existing_forms:
            new_forms.append(existing_forms[key])
        else:
            instance = instance_mapping[instance_name]
            interface = interface_mapping[(interface_name, interface_version)]
            new_forms.append(
                services.form.create_form(instance=instance, interface=interface)
            )

    for stale in set(existing_forms.values()) - set(new_forms):
        for group_association in stale.group_associations:
            model.conn.delete(group_association)
        model.conn.delete(stale)


def import_instances(app, instances):
    return collection_diff(
        obj=app,
        collection_name='instances',
        loader=lambda: {
            import_instance(app, name, attrs)
            for name, attrs in instances.items()
        }
    )


def import_interfaces(app, interfaces):
    return collection_diff(
        obj=app,
        collection_name='interfaces',
        loader=lambda: {
            import_interface(app, name, attrs)
            for name, attrs in interfaces.items()
        }
    )


def import_interface(app, internal_name, attributes):
    try:
        interface = services.application_interface.application_interface_by_internal_name(internal_name=internal_name)
        interface = services.application_interface.update_application_interface(interface, attributes)
    except NoResultFound:
        interface = services.application_interface.create_application_interface(
            application=app,
            name=attributes.get('name'),
            internal_name=internal_name,
            version=attributes.get('version'),
            description=attributes.get('description'),
            input_files=[],
            input_values=[]
        )

    if attributes.get('document'):
        if not interface.document:
            interface.document = model.misc.Document()
            model.conn.add(interface.document)
        interface.document.content = attributes['document'].encode()
    else:
        if interface.document:
            interface.document = None

    import_interface_inputs(
        interface=interface,
        input_files=attributes.get('inputFiles') or {},
        input_values=attributes.get('inputValues') or {},
    )

    return interface


def import_input_file_definition(interface, internal_name, attributes):
    try:
        definition = services.application_interface.file_definition_by_internal_name(interface=interface,
                                                                                     internal_name=internal_name)
        definition = services.application_interface.update_file_definition(definition, attributes)
    except NoResultFound:
        definition = model.interface.InputFileDefinition(internal_name=internal_name, **attributes)
        model.conn.add(definition)

    return definition


def import_input_value_definition(interface, internal_name, attributes):
    mapping = {
        'enumerations': ('allowed_binaries', lambda values: [json.dumps(v).encode() for v in values]),
        'default': ('default_binary', lambda v: json.dumps(v).encode()),
    }

    attributes = dict(
        (mapping[k][0], mapping[k][1](v)) if k in mapping else (k, v)
        for k, v in attributes.items()
    )

    try:
        definition = services.application_interface.value_definition_by_internal_name(interface=interface,
                                                                                      internal_name=internal_name)
        definition = services.application_interface.update_value_definition(definition, attributes)
    except NoResultFound:
        definition = model.interface.InputValueDefinition(internal_name=internal_name, **attributes)
        model.conn.add(definition)

    return definition


def import_interface_inputs(interface, input_files, input_values):
    collection_diff(
        obj=interface,
        collection_name='input_file_definitions',
        loader=lambda: {
            import_input_file_definition(interface, name, attrs)
            for name, attrs in input_files.items()
        }
    )

    collection_diff(
        obj=interface,
        collection_name='input_value_definitions',
        loader=lambda: {
            import_input_value_definition(interface, name, attrs)
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
        str(sched['value']),
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


def import_groups(applications):
    created_groups = {}
    for application, properties in applications.items():
        for form_info in properties.get('forms', []):
            groups = form_info.get('groups') or []
            instance_name, interface_name, interface_version = (
                form_info['instance'], form_info['interface']['internalName'], form_info['interface']['version']
            )
            instance = model.application.ApplicationInstance.query().filter_by(internal_name=instance_name).one()
            interface = model.interface.ApplicationInterface.query().filter_by(
                internal_name=interface_name,
                version=interface_version
            ).one()

            form: model.interface.Form = model.interface.Form.query().filter_by(
                instance_id=instance.id, interface_id=interface.id
            ).one()

            associations = {assoc.group.name: assoc for assoc in form.group_associations}
            stale_assocs = set(associations) - set(groups)
            for stale in stale_assocs:
                model.conn.delete(associations[stale])

            for group_name in groups:
                if group_name in associations:
                    continue
                group = model.group.Group.query().filter_by(name=group_name).first()
                if not group:
                    group = created_groups.get(group_name)
                if not group:
                    group = services.auth.create_group(name=group_name)
                    created_groups[group_name] = group

                association = model.group.ResourceGroupAssociation(
                    resource_id=form.id,
                    resource_type=model.group.ResourceType.form,
                    group=group
                )
                model.conn.add(association)


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


def import_config(config, formatter=None, restrict_apps=None):
    applications = config.get('applications') or {}
    import_applications(applications=applications, restrict=restrict_apps)

    diff = session_diff()
    model.conn.flush()

    import_groups(applications=applications)

    for k, v in session_diff().items():
        if k in diff:
            diff[k] = diff[k][0], {**diff[k][1], **v[1]}
        else:
            diff[k] = v

    model.conn.flush()
    return formatter(diff) if formatter else diff


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
