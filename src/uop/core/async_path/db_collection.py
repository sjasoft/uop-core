__author__ = "samantha"

from functools import partial
from uop.core import db_collection as base


unique_field = lambda name: partial(base.UniqueField, name)


class DatabaseCollections(base.DatabaseCollections):
    def __init__(self, db):
        super().__init__(db)

    async def metadata(self):
        return {k: await self._collections[k].find() for k in base.shared_collections}

    async def drop_collections(self, collections):
        for col in collections:
            await col.drop()

    async def class_extension(self, cls_id):
        cls = await self.classes.get(cls_id)
        return await self.get_class_extension(cls)

    async def extension(self, cls):
        name = cls.get(base.cls_extension_field)
        if not name:
            name = self._db.new_collection_name()
            cls[base.cls_extension_field] = name
            await self.classes.update_one(cls["id"], {base.cls_extension_field: name})
        return name

    async def get_class_extension(self, cls):
        name = await self.extension(cls)
        cid = cls["id"]
        coll = self._extensions.get(cid)
        if not coll:
            coll = await self._db.get_managed_collection(name, schema=cls)
            self._extensions[cid] = coll
        return coll

    async def ensure_class_extensions(self):
        classes = await self.classes.find()
        for cls in classes:
            await self.get_class_extension(cls)

    async def ensure_collections(self, col_map):
        for name in col_map:
            if not self._collections.get(name):
                schema = base.kind_map.get(name)
                self._collections[name] = await self._db.get_managed_collection(
                    col_map[name], schema
                )

    def get(self, name):
        return self._collections.get(name)


class DBCollection(base.DBCollection):
    """Abstract collection base."""

    async def count(self, criteria):
        data = await self.find(criteria)
        return len(data)

    async def ensure_index(self, coll, *attr_order):
        pass

    async def distinct(self, key, criteria):
        pass

    async def update(self, selector, mods, partial=True):
        pass

    async def drop(self):
        cond = {}
        if cond:
            await self.remove(cond)
        else:
            await self._coll.drop()

    async def insert(self, **fields):
        pass

    async def bulk_load(self, *ids):
        pass

    async def remove(self, dict_or_key):
        pass

    async def remove_all(self):
        return await self.remove({})

    async def remove_instance(self, instance_id):
        return await self.remove(instance_id)

    async def find(
        self, criteria=None, only_cols=None, order_by=None, limit=None, ids_only=False
    ):
        return []

    async def all(self):
        return await self.find()

    async def ids_only(self, criteria=None):
        return await self.find(criteria=criteria, only_cols=[self.ID_Field])

    async def find_one(self, criteria, only_cols=None):
        res = await self.find(criteria, only_cols=only_cols, limit=1)
        return res[0] if res else None

    async def exists(self, criteria):
        return await self.count(criteria)

    async def contains_id(self, an_id):
        return await self.exists({"id": an_id})

    async def get(self, instance_id):
        data = None
        if self._indexed:
            data = self._by_id.get(instance_id)
        if not data:
            data = await self.find_one({"id": instance_id})
        if data and self._indexed:
            self._index(data)
        return data

    async def all(self):
        return await self.find()

    async def get_all(self):
        """
        Returns a dictionary of mapping record ids to records for all
        records in the collection
        :return: the mapping
        """
        return {x["_id"]: x async for x in self.find()}

    async def instances(self):
        return await self.find()

    async def replace_one(self, an_id, data):
        await self._coll.replace_one({"id": an_id}, data)

    async def replace(self, object):
        id = object.pop("id")
        return await self.replace_one(id, object)
