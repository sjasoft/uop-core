__author__ = "samantha"

from collections import defaultdict
from sjasoft.uopmeta import oid
from sjasoft.uopmeta import attr_info
from sjasoft.uopmeta.schemas.meta import MetaContext, Schema, Related

from sjasoft.uopmeta.attr_info import assoc_kinds, meta_kinds, crud_kinds
from sjasoft.uopmeta.oid import id_field


get_id = lambda data: data[id_field]


def oid_matches(to_check, oid):
    return to_check == oid


class ChangeSetComponent(object):
    def __init__(self, changeset):
        self._changeset = changeset


class RelatedChanges(ChangeSetComponent):
    _object_fields = "object_id", "subject_id"
    kind = "related"

    def __init__(self, changeset, data=None):
        data = data or {}
        items = lambda key: data.get(key, set())
        self.inserted: set = {Related(d) for d in items("inserted")}
        self.deleted: set = {Related(d) for d in items("deleted")}
        ChangeSetComponent.__init__(self, changeset)

    def clear(self):
        self.inserted.clear()
        self.deleted.clear()

    def has_changes(self):
        return any([self.inserted, self.deleted])

    def add_changes(self, other):
        """
        Add subsequent changes to the existing changes
        :param other: the other changeset component
        :return: None
        """
        deleted_objects = (
            self._changeset.objects.deleted | other._changeset.objects.deleted
        )
        deleted_classes = (
            self._changeset.classes.deleted | other._changeset.classes.deleted
        )
        deleted_tags = self._changeset.tags.deleted | other._changeset.tags.deleted
        deleted_groups = (
            self._changeset.groups.deleted | other._changeset.groups.deleted
        )
        deleted_roles = self._changeset.roles.deleted | other._changeset.roles.deleted

        self.deleted.update(other.deleted)
        self.inserted.update(other.inserted)
        self.inserted -= self.deleted
        for obj_id in deleted_objects:
            self.delete_object(obj_id)
        for cls_id in deleted_classes:
            self.delete_class(cls_id)
        for tag_id in deleted_tags:
            test = lambda x: x == tag_id
            self.memory_filter(test)
        for group_id in deleted_groups:
            test = lambda x: x == group_id
            self.memory_filter(test)
        for role_id in deleted_roles:
            self.delete_association(role_id)

    def to_dict(self):
        return dict(
            inserted=[d.dict() for d in self.inserted],
            deleted=[d.dict() for d in self.deleted],
        )

    def insert(self, data):
        item = Related(**data) if isinstance(data, dict) else data
        self.inserted.add(item)
        return item

    def delete(self, data, unused_changset=None):
        data = Related(**data) if isinstance(data, dict) else data
        if data in self.inserted:
            self.inserted.discard(data)
        else:
            self.deleted.add(data)

    def memory_filter(self, object_field_test):
        self.inserted = {
            x
            for x in self.inserted
            if not (object_field_test(x.subject_id) or object_field_test(x.object_id))
        }
        self.deleted = {
            x
            for x in self.deleted
            if not (object_field_test(x.subject_id) or object_field_test(x.object_id))
        }

    def delete_object(self, object_id):
        test = lambda x: x == object_id
        self.memory_filter(test)

    def delete_class(self, cls_id):
        test = lambda x: oid.oid_class(x) == cls_id
        self.memory_filter(test)

    def delete_association(self, assoc_id):
        self.inserted = {x for x in self.inserted if x.assoc_id != assoc_id}
        self.deleted = {x for x in self.deleted if x.assoc_id != assoc_id}


class CrudChanges(ChangeSetComponent):
    kind = "_"

    # classmethod
    def user_collection(cls, collections):
        return collections[cls.kind]

    def __init__(self, changeset, data=None):
        data = data or {}
        self.inserted = data.get("inserted", defaultdict(dict))
        self.modified = data.get("modified", defaultdict(dict))
        self.deleted = set(data.get("deleted", []))
        ChangeSetComponent.__init__(self, changeset)

    def expand_changed(self, dbi):
        res = super().expand_changed()
        for oid, mods in self.modified.items():
            data = dbi.get_object(oid)
            if data:
                data.update(mods)
                res.append(data)
        return res

    def has_changes(self):
        return any([self.inserted, self.modified, self.deleted])

    def __copy__(self):
        return self.__class__(self.kind, self.to_dict())

    def delete(self, identifier, in_changeset=None):
        """
        Rationalize deletes against insertions and modifications.
        If item is in the insert of the changes then remove it.
        Remove it if it is in the modified set otherwise.
        Add it to the deleted set if it was not in the inserted set
        modify other parts of the overall changeset for the kind
        of item being deleted.
        :param identifier: id of the item being deleted
        :param in_changeset: the containing changeset.
        :return: None
        """
        if not self.inserted.pop(identifier, None):
            self.modified.pop(identifier, None)
            self.deleted.add(identifier)
        self.handle_delete(identifier, in_changeset)

    def handle_delete(self, identifier, changeset):
        pass

    def modify(self, identifier, data):
        "Rationalize modifications"
        if identifier in self.inserted:
            self.inserted[identifier].update(data)
            return self.inserted[identifier]
        elif identifier in self.modified:
            self.modified[identifier].update(data)
        else:
            self.modified[identifier] = data
        return None

    def insert(self, data):
        self.inserted[get_id(data)] = data

    def to_dict(self):
        return dict(
            inserted=self.inserted, modified=self.modified, deleted=list(self.deleted)
        )

    def add_changes(self, other_changes, in_changeset):
        self.inserted.update(other_changes.inserted)
        for k, v in other_changes.modified.items():
            self.modify(k, v)
        for _id in other_changes.deleted:
            self.delete(_id, in_changeset)

    def clear(self):
        self.inserted.clear()
        self.modified.clear()
        self.deleted.clear()


class ObjectChanges(CrudChanges):
    kind = "objects"

    def delete(self, identifier, in_changeset=None):
        super().delete(identifier, in_changeset)

    def handle_delete(self, identifier, in_changeset):
        in_changeset.related.delete_object(identifier)

    def deletion_criteria(self, uuid):
        return {"$or": [{"object_id": uuid}, {"subject_id": uuid}]}

    def delete_class(self, class_id):
        test_class = lambda uuid: uuid.split(".") != class_id
        self.inserted = dict(
            [(k, v) for k, v in self.inserted.items() if test_class(k)]
        )
        self.modified = dict(
            [(k, v) for k, v in self.modified.items() if test_class(k)]
        )
        self.deleted = {s for s in self.deleted if test_class(s)}


class RoleChanges(CrudChanges):
    kind = "roles"

    def deletion_criteria(self, key):
        return {"assoc_id": key}

    def delete(self, identifier, in_changeset=None):
        super().delete(identifier, in_changeset)
        in_changeset.related.delete_association(identifier)


class TagChanges(CrudChanges):
    kind = "tags"

    def deletion_criteria(self, key, role_id):
        return {"subject_id": key, "assoc_id": role_id}


class GroupChanges(CrudChanges):
    kind = "groups"

    def containing_criteria(self, key, role_id):
        return {"subject_id": key, "assoc_id": role_id}

    def contained_criteria(self, key, role_id):
        return {"object_id": key, "assoc_id": role_id}


class QueryChanges(CrudChanges):
    kind = "queries"
    pass


class ClassChanges(CrudChanges):
    kind = "classes"

    def deletion_criteria(self, key):
        filter = lambda fld: {"$regex": {fld: f"^{key}\\."}}
        obj_check = filter("object_id")
        subject_check = filter("subject_id")
        return {"$or": [obj_check, subject_check]}

    def handle_delete(self, identifier, in_changeset):
        in_changeset.objects.delete_class(identifier)
        in_changeset.related.delete_class(identifier)


class AttributeChanges(CrudChanges):
    kind = "attributes"
    pass

    def db_not_dup(self, collection, data):
        checked_data = dict(name=data["name"], type_id=data["type_id"])
        return not collection.exists(checked_data)


class ChangeSet(object):
    change_types = dict(
        objects=ObjectChanges,
        roles=RoleChanges,
        tags=TagChanges,
        groups=GroupChanges,
        classes=ClassChanges,
        attributes=AttributeChanges,
        queries=QueryChanges,
    )

    def __init__(self, **data):
        """
        Creates a changeset in internal form from a changeset in external form
        :param data: changeset with all sets made lists that is json compatible
        """
        self.objects = ObjectChanges(self, data.get("objects"))
        self.roles = RoleChanges(self, data.get("roles"))
        self.tags = TagChanges(self, data.get("tags"))
        self.groups = GroupChanges(self, data.get("groups"))
        self.classes = ClassChanges(self, data.get("classes"))
        self.attributes = AttributeChanges(self, data.get("attributes"))
        self.related = RelatedChanges(self, data.get("related"))
        self.queries = QueryChanges(self, data.get("queries"))

    def has_changes(self):
        fields = [
            "objects",
            "roles",
            "tags",
            "groups",
            "grouped",
            "classes",
            "attributes",
            "tagged",
            "related",
        ]
        changes = {f: getattr(self, f).has_changes() for f in fields}
        return any(changes.values())

    def to_dict(self):
        return dict([(key, getattr(self, key).to_dict()) for key in self.change_types])

    def object_deleted(self, obj_id):
        cls_id = oid.oid_class(obj_id)
        return (cls_id in self.classes.deleted) or (obj_id in self.objects.deleted)

    def add_changes(self, other_changes: "ChangeSet"):
        for kind in crud_kinds:
            data = getattr(other_changes, kind)
            for inserted in data.inserted.values():
                self.insert(kind, inserted)
            for k, v in data.modified.items():
                self.modify(kind, k, v)
            for k in data.deleted:
                self.delete(kind, k)
        self.related.add_changes(other_changes.related)

    @classmethod
    def combine_changes(cls, *changesets):
        """
        combines sequential changesets into one
        :param changesets: sequence of changeset in dict form
        :return: combined changeset
        """

        def as_changeset(changes):
            return changes if isinstance(changes, ChangeSet) else cls(**changes)

        def as_dict(changes):
            return changes.to_dict() if isinstance(changes, ChangeSet) else changes

        combined = cls(**as_dict(changesets[0]))  # copy of first changeset
        for cs in changesets[1:]:
            combined.add_changes(as_changeset(cs))
        return combined

    def clear(self):
        for kind in self.change_types:
            getattr(self, kind).clear()

    def insert(self, kind, data):
        try:
            coll = getattr(self, kind, self.related)
        except Exception as e:
            raise e
        coll.insert(data)

    def modify(self, kind, an_id, data):
        coll = getattr(self, kind, self.related)
        return coll.modify(an_id, data)

    def delete(self, kind, an_id):
        coll = getattr(self, kind, self.related)
        coll.delete(an_id, self)


def meta_context_as_changeset(context: MetaContext):
    """
    Builds a changeset matching the context. This is primarily used
    for storage of a new Application's metadata.
    """
    changes = ChangeSet()
    for kind in meta_kinds:
        instances = getattr(context, kind, [])
        for instance in instances:
            changes.insert(kind, instance.dict(exclude_none=True))
    return changes


def context_to_schema_changeset(context: MetaContext, schema: Schema):
    changes = ChangeSet()
    for kind in meta_kinds:
        s_instances = getattr(schema, kind, [])
        for inst in s_instances:
            pass


def meta_context_schema_diff(context: MetaContext, a_schema):
    changes = ChangeSet()
    context.gather_schema_changes(a_schema, changes)
    return changes
