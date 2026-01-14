from uop.core.db_service import get_uop_service, DatabaseClass, UOPContext
from uop.core.connect import generic
from uop.meta import oid
from uop.meta.schemas import meta
from functools import reduce


def register_adaptor(db_class, db_type, is_async=False):
    DatabaseClass.register_db(db_class, db_type, is_async=is_async)


class ConnectionWrapper:
    def __init__(self, connect: generic.GenericConnection = None):
        self._connect = connect

    def set_connection(self, connect: generic.GenericConnection):
        self._connect = connect

    def abort(self):
        self._connect.abort()

    def begin_transaction(self):
        self._connect.begin_transaction()

    def class_named(self, name):
        return self._connect.metacontext().classes.by_name.get(name)

    def __getattr__(self, name):
        return getattr(self._connect, name, None)

    def create_instance(self, cls, **data):
        return self.create_instance_of(cls.name, use_defaults=True, **data)

    def commit(self):
        self._connect.commit()

    def get_dataset(self, num_assocs=3, num_instances=10, persist_to=None):
        # assume metacontext is complete
        data = meta.WorkingContext.from_metadata(self._connect.metacontext())
        data.configure(
            num_assocs=num_assocs, num_instances=num_instances, persist_to=persist_to
        )
        return data

    def dataset(self, num_assocs=3, num_instances=10, persist=None):
        persist_to = None
        if persist:
            self.dbi.begin_transaction()
            persist_to = self.dbi
        data = self.get_dataset(
            num_assocs=num_assocs, num_instances=num_instances, persist_to=persist_to
        )
        if persist:
            self.dbi.commit()
        return data

    def get_db_method(self, name):
        return getattr(self, name)

    def get_role_named(self, name):
        return self._connect.metacontext().get_meta_named("roles", name)

    def meta_map(self):
        data = self._connect.metacontext().__dict__
        kinds = {k: v for k, v in data.items() if isinstance(v, meta.ByNameId)}
        return {k: v.by_id for k, v in kinds.items()}

    def metacontext(self):
        return self._connect.metacontext()

    def object_attributes(self, obj_id):
        _cls = self.object_class(obj_id)
        if _cls:
            return _cls.attributes
        raise Exception(f"No class for object {obj_id}")

    def object_class(self, obj_id):
        cid = oid.oid_class(obj_id)
        return self.id_map("classes").get(cid)

    def object_display_info(self, obj_id):
        cid = oid.oid_class(obj_id)
        cname = self.id_to_name("classes")(cid)
        short = self.dbi.oid_short_form(obj_id)
        return dict(short_form=short, class_name=cname)

    def reverse_role_names(self):
        return [r.reverse_name for r in self.roles()]

    def roles(self):
        return self.name_map("roles").values()

    def rolesets(self, obj, rids):
        getter = self.roleset_getter(obj)
        return {r: getter(r) for r in rids}

    def subgroups(self, gid):
        return self._connect.metacontext().subgroups(gid)

    def untag(self, oid, tag_id):
        self._connect.untag(oid, tag_id)

    def ungroup(self, oid, group_id):
        self._connect.ungroup(oid, group_id)

    def unrelate(self, subject, role_id, object_id):
        self._connect.unrelate(subject, role_id, object_id)

    def url_to_object(self, url):
        return self.dbi.object_for_url(url, True)

    def name_map(self, kind):
        return self._connect.metacontext().by_name(kind)

    def id_map(self, kind):
        return self._connect.metacontext().by_id(kind)

    def id_to_name(self, kind):
        return self._connect.metacontext().id_to_name(kind)

    def names_from_ids(self, kind, *ids):
        return self._connect.metacontext().names_to_ids(kind)(ids)

    def name_to_id(self, kind):
        return self._connect.metacontext().name_to_id(kind)

    def neighbor_text_form(self, kind, neighbor_dict):
        """
        Internal neighbor form has id nodes(key) and list of object ids leaves.
        The corresponding text form has corresponding meta object name nodes and object short form leaves.
        """
        name_map = self.id_to_name(kind)
        unique_objects = reduce(lambda a, b: a & set(b), neighbor_dict.values(), set())
        short_map = {}
        for oid in unique_objects:
            short_map[oid] = self._connect.object_short_form(self.get_object(oid))

        def short_objs(oids):
            return [short_map[o] for o in oids]

        return {name_map[k]: short_objs(v) for k, v in neighbor_dict.items()}
