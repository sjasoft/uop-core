from uop.meta.schemas import meta
from uop.core.collections import crud_kinds, assoc_kinds


def crud_names(base):
    return "add_" + base, "modify_" + base, "delete_" + base


interface_methods = dict(
    classes=crud_names("class"),
    roles=crud_names("role"),
    attributes=crud_names("attribute"),
    tags=crud_names("tag"),
    groups=crud_names("group"),
    tagged=("tag", "untag"),
    grouped=("group", "ungroup"),
    related=("relate", "unrelate"),
    objects=crud_names("object"),
)


class Plugin:
    def __init__(self, db_plugin):
        self.plugin = db_plugin
        self.get_object = self.plugin.get_object

    def _get_method(self, name):
        return getattr(self.plugin, name)

    def get_kind_collection(self, kind):
        return getattr(self.plugin.collecttions, kind)

    def get_methods(self, kind):
        method_names = interface_methods[kind]
        methods = [self._get_method(m) for m in method_names]
        kv = zip(["insert", "modify", "delete"], methods)
        return dict(kv)

    def object_exists(self, obj_id):
        return self.get_object(obj_id) is not None

    def meta_item_exists(self, kind, an_id):
        return self.get_kind_collection(kind).get(an_id)

    def check_collections(self):
        """just ensure collection existence, and separation across user and regular dbs"""
        for kind in meta.kind_map:
            # TODO maybe expand here to test tenant case
            collection = self.get_kind_collection(kind)
            assert collection

    def get_unique(self, count, random_fn, known):
        """
        Get count unique items from random_fn, not in known
        :param count: number of items to get
        :param random_fn: function to get random items
        :param known: list of dicts  of known items
        :return: list of unique items
        """
        res = []

        def to_tuple(item):
            as_dict = item if isinstance(item, dict) else item.dict()
            return tuple(as_dict.values())

        known = set(map(to_tuple, known))
        while len(res) < count:
            item = random_fn()
            as_tuple = to_tuple(item)
            if as_tuple not in known:
                res.append(item)
                known.add(as_tuple)
        return res

    def insert_and_check(self, random_data, db_tagged, db_grouped, db_related):
        for kind in crud_kinds:
            if kind in ["objects", "queries"]:
                continue
            cls = meta.kind_map[kind]
            inserter = self.get_methods(kind)["insert"]
            coll = self.get_kind_collection(kind)
            data = random_data.all_of_kind(kind)
            for obj in data:
                id = obj.id
                present = coll.get(id)
                if present:
                    print("already in database", kind, id, obj)
                else:
                    data = obj.without_kind()
                    inserter(**data)
                from_db = coll.get(id)
                if not from_db:
                    print("%s(%s) not in db!" % (kind, id))
                assert from_db
        assoc_add = random_data.random_tagged()
        for kind in assoc_kinds:
            fn = getattr(random_data, f"random_{kind}")
            coll = self.get_kind_collection(kind)
            items = self.get_unique(5, fn, coll.find())
            for obj in items:
                obj = obj.without_kind()
                coll.insert(**obj)
                found = coll.find_one(obj)
                assert found

    def modify_and_check(self, random_data, db_tagged, db_grouped, db_related):
        global context
        desc = "this is the new description"
        for kind in crud_kinds:
            if kind in ["objects", "queries"]:
                continue
            cls = meta.kind_map[kind]
            modifier = self.get_methods(kind)["modify"]
            coll = self.get_db_method(kind)
            for obj in random_data.all_of_kind(kind):
                id = obj.id
                modifier(id, description=desc)
                from_db = coll.get(id)
                if not from_db:
                    print("%s(%s) no in db!" % (kind, id))
                assert from_db["description"] == desc

    get_id = lambda obj: obj["id"]

    def delete_and_check(self, random_data, db_tagged, db_grouped, db_related):
        global context
        a_class, other_class = random_data.distinct_pair(
            "classes", lambda c: not c.is_abstract
        )
        random_class = random_data.random_new_class()
        context.get_db_method("add_class")(**random_class.without_kind())
        a_role, another_role = random_data.distinct_pair("roles")
        a_tag, another_tag = random_data.distinct_pair("tags")
        a_group, another_group = random_data.distinct_pair("groups")

        add_object = self.get_db_method("add_object")

        def add_class_object(a_class):
            global context
            object = random_data.random_class_instance(a_class)
            insert = self.get_db_method("add_object")
            insert(object)
            return object

        def add_grouped(group, object) -> meta.Grouped:
            global context
            assoc = meta.Grouped(assoc_id=group.id, object_id=self.get_id(object))
            db_group = self.get_db_method("group")
            db_group(self.get_id(object), group.id)
            return assoc

        def add_tagged(tag, object) -> meta.Tagged:
            global context
            assoc = meta.Tagged(assoc_id=tag.id, object_id=self.get_id(object))
            db_tag = self.get_db_method("tag")
            db_tag(self.get_id(object), tag.id)
            return assoc

        def add_related(role, subject, object) -> meta.Related:
            global context
            assoc = meta.Related(
                assoc_id=role.id,
                object_id=self.get_id(object),
                subject_id=self.get_id(subject),
            )
            db_relate = context.get_db_method("relate")
            db_relate(self.get_id(subject), role.id, self.get_id(object))
            return assoc

        def assoc_exists(collection, assoc: meta.Associated):
            data = assoc.dict()
            data.pop("kind", None)
            return collection.exists(data)

        obj1 = add_class_object(a_class)
        assert self.object_exists(obj1["id"])
        obj2 = add_class_object(a_class)
        obj3 = add_class_object(other_class)
        obj4 = add_class_object(random_class)
        obj5 = add_class_object(random_class)

        assert self.object_exists(obj1["id"])

        grouped = add_grouped(a_group, obj1)
        grouped2 = add_grouped(a_group, obj2)
        grouped3 = add_grouped(another_group, obj2)
        grouped4 = add_grouped(another_group, obj4)

        tagged = add_tagged(a_tag, obj1)
        tagged2 = add_tagged(a_tag, obj2)
        tagged3 = add_tagged(another_tag, obj2)
        tagged4 = add_tagged(another_tag, obj4)

        related = add_related(a_role, obj1, obj2)
        related2 = add_related(a_role, obj2, obj2)
        related3 = add_related(another_role, obj2, obj4)
        related4 = add_related(another_role, obj3, obj4)

        self.get_methods("objects")["delete"](self.get_id(obj1))
        assert not assoc_exists(db_grouped, grouped)
        assert assoc_exists(db_grouped, grouped2)
        assert not assoc_exists(db_tagged, tagged)
        assert assoc_exists(db_tagged, tagged2)
        assert not assoc_exists(db_related, related)
        assert assoc_exists(db_related, related2)

        self.get_methods("classes")["delete"](random_class.id)
        assert not assoc_exists(db_grouped, grouped4)
        assert assoc_exists(db_grouped, grouped2)
        assert not assoc_exists(db_tagged, tagged4)
        assert assoc_exists(db_tagged, tagged2)
        assert not assoc_exists(db_related, related3)
        assert not assoc_exists(db_related, related4)
        assert assoc_exists(db_related, related2)

        self.get_methods("groups")["delete"](another_group.id)
        assert not self.get_kind_collection("grouped").exists(grouped4.without_kind())
        self.get_methods("tags")["delete"](another_tag.id)
        assert not self.get_kind_collection("tagged").exists(tagged4.without_kind())
        self.get_methods("roles")["delete"](another_role.id)
        assert not self.get_kind_collection("related").exists(related4.without_kind())
