from collections import defaultdict

comment = defaultdict(set)
from sjasoft.utils.decorations import abstract
import time
from sjasoft.utils import index
from sjasoft.utils import decorations
from uop.core.async_path import changeset
from uop.meta import oid
from uop.meta.schemas import meta
from uop.core.async_path import db_collection as db_coll
from uop.core import interface as iface
from sjasoft.utils.index import make_id
import asyncio
from uop.core import database as base

logger = base.logger


class Database(base.Database):
    async def get_metadata(self):
        return await self.collections.metadata()

    async def reload_metacontext(self):
        coll_meta = await self.get_metadata()
        self._context = meta.MetaContext.from_data(coll_meta)

    async def open_db(self, setup=None):
        self._collections = db_coll.DatabaseCollections(self)
        colmap = base.uop_collection_names
        if self._tenant_id:
            self._tenant = await self.get_tenant(self._tenant_id)
            if self._tenant:
                colmap.update(self._tenant.base_collections)
        await self._collections.ensure_collections(colmap)
        await self._collections.ensure_class_extensions()
        self._collections_complete = True

        await self.reload_metacontext()

    async def get_tenant(self, tenant_id):
        tenants = await self.tenants()
        return await tenants.get(tenant_id)

    async def make_random_collection(self):
        res = index.make_id(48)
        if not res[0].isalpha():
            res = "x" + res
        return await self.get_managed_collection(res)

    async def drop_tenant(self, tenant_id):
        """
        Drops the tenant from the database.  This version removes their data.
        :param tenant_id id of the tenant to remove
        """
        collections = await self.get_tenant_collections(tenant_id)
        if collections:
            await collections.drop_collections(collections)

    async def ensure_indices(self, indices):
        pass

    async def gew_raw_collection(self, name):
        pass

    async def get_managed_collection(self, name, schema=None):
        known = self.collections.get(name)
        if not known:
            raw = await self.get_raw_collection(name, schema)
            known = self.wrap_raw_collection(raw)
        return known

    @property
    def collections(self):
        if not self._collections:
            self._collections = db_coll.DatabaseCollections(self)
        return self._collections

    async def ensure_database_info(self):
        db_info = self.db_info()
        db = self.database_collection()
        if not db_info:
            db_info = await db.insert(_id=self._id, tenancy=self._tenancy)
        return db_info

    async def db_info(self):
        if not self._db_info:
            db = self.database_collection()
            self._db_info = await db.get(self._id)
        return self._db_info

    async def get_tenant_collections(self, tenant_id=None):
        """
        Returns a db collections object for the given tenant_id if there
        is such a teannt
        :param tenant_id: id of the tenet
        :return: DBCollections instance or None
        """
        await self.ensure_basic_collections()
        collections = self.collections
        if tenant_id:
            tenant = await self.get_tenant(tenant_id)
            if tenant:
                collections = self._tenant_map.get(tenant_id)
                if not collections:
                    col_map = tenant.get("collections_map")
                    collections = db_coll.DatabaseCollections(self, tenant_id=tenant_id)
                    await collections.ensure_basic_collections(col_map)
                    self._tenant_map[tenant_id] = collections
        return collections

    async def log_changes(self, changeset, tenant_id=None):
        """Log the changeset.
        We could log external to the main database but here we will presume that
        logging is local.
        """
        changes = meta.MetaChanges(
            timestamp=time.time(), tenant_id=tenant_id, changes=changeset.to_dict()
        )
        coll = self.collections.changes
        await coll.insert(**changes.dict())

    @base.contextmanager
    async def changes(self, changeset=None):
        changes = self._changeset or changeset.ChangeSet()
        yield changes
        if not self._changeset:
            await self.apply_changes(changes)

    async def changes_since(self, epochtime, tenant_id, client_id=None):
        tenant_id = tenant_id or 0
        client_id = client_id or 0
        criteria = changeset.changes.criteria(epochtime, tenant_id, client_id)
        changesets = await self.collections.changes.find(
            criteria, order_by=("timestamp",), only_cols=("changes",)
        )
        return changeset.ChangeSet.combine_changes(*changesets)

    async def apply_changes(self, changeset):
        extensions_to_remove = []

        async def delete_class(cls_id):
            coll = self.extension(cls_id)
            extensions_to_remove.append(coll.name)
            criteria = changeset.classes.deletion_criteria(cls_id)
            await self.collections.related.remove(criteria)

        async def delete_attribute(attr_id):
            pass

        async def delete_role(role_id):
            criteria = changeset.roles.deletion_criteria(role_id)
            await self.collections.related.remove(criteria)

        async def delete_tag(tag_id):
            criteria = changeset.tags.deletion_criteria(
                tag_id, self.role_id("tag_applies")
            )
            await self.collections.related.remove(criteria)

        async def delete_group(group_id):
            containing_role_id = self.role_id("group_contains")
            contains_criteria = changeset.groups.containing_criteria(
                group_id, containing_role_id
            )
            await self.collections.related.remove(contains_criteria)
            contained_role_id = self.role_id("contains_group")
            contained_criteria = changeset.groups.contained_criteria(
                group_id, contained_role_id
            )
            await self.collections.related.remove(contained_criteria)

        async def delete_object(object_id):
            criteria = changeset.objects.deletion_criteria(object_id)
            await self.collections.related.remove(criteria)

        async def delete_query(query_id):
            pass

        delete_completions = dict(
            classes=delete_class,
            attributes=delete_attribute,
            roles=delete_role,
            tags=delete_tag,
            groups=delete_group,
            objects=delete_object,
            queries=delete_query,
        )

        async def apply_meta_changes(changes):
            coll = getattr(self.collections, changes.kind)
            for k, v in changes.inserted.items():
                await coll.insert(**v)
            for k, v in changes.modified.items():
                await coll.update_one(k, v)
            for k in changes.deleted:
                coll.remove(k)
                await delete_completions[changes.kind](k)

        async def apply_related_changes(changes):
            for related in changes.inserted:
                await self.collections.related.insert(**dict(related))
            for related in changes.deleted:
                await self.collections.related.remove(dict(related))

        self.begin_transaction()
        for kind in base.crud_kinds:
            await apply_meta_changes(getattr(changeset, kind))
        apply_related_changes(changeset.related)

        for extension_name in extensions_to_remove:
            self.collections.remove(extension_name)
        await self.log_changes(changeset)
        await self.commit()
        await self.reload_metacontext()

    async def commit(self):
        await self._db.commit()

    async def ensure_core_schema(self):
        await self.ensure_schema(meta.core_schema)

    async def ensure_schema(self, a_schema: meta.Schema):
        if not await self.schemas().find_one({"name": a_schema.name}):
            await self.add_schema(a_schema)
        await self.ensure_schema_installed(a_schema)        

    async def ensure_schema_installed(self, a_schema):
        changes = changeset.meta_context_schema_diff(self.metacontext, a_schema)
        has_changes = changes.has_changes()
        if has_changes:
            await self.apply_changes(changes)
            await self.reload_metacontext()
        return has_changes, changes

    async def object_ok(self, object_id):
        cls_id = oid.oid_class(object_id)
        if self.class_ok(cls_id):
            coll = await self.extension(cls_id)
            return await coll.contains_id(object_id)
        return False

    async def add_schema(self, a_schema: meta.Schema):
        """
        Adds a schema to the database.
        :param a_schema: a Schema
        :return: None
        """
        await self.schemas().insert(**a_schema.dict())

    async def extension(self, cls_id):
        return await self.collections.class_extension(cls_id)

    async def add_tenant(self, tenant: meta.Tenant):
        tenants = self.tenants()
        tenant = tenants.insert(**tenant.dict())
        return tenant

    async def add_tenant_user(self, tenant_id: str, user_id: str):
        await self.relate(tenant_id, self.role_id["has_user"], user_id)

    async def remove_tenant_user(self, tenant_id: str, user_id: str):
        await self.unrelate(tenant_id, self.role_id["has_user"], user_id)

    async def drop_tenant(self, tenant_id):
        """
        Drops the tenant from the database.  This version removes their data.
        :param tenant_id id of the tenant to remove
        """
        collections = self.get_tenant_collections(tenant_id)
        if collections:
            await self.collections.drop_collections(collections)

    async def get_tenant_collections(self, tenant_id):
        tenant = await self.get_tenant(tenant_id)
        return tenant.base_collections

    async def create_tenant(self, name=""):
        tenant = meta.Tenant(name=name)
        for kind in base.per_tenant_kinds:
            tenant.base_collections[kind] = self.random_collection_name()
        self.add_tenant(tenant)
        return tenant
    
    @contextmanager
    async def perhaps_committing(self, commit=False):
        yield
        if commit:
            await self.commit()

    async def start_long_transaction(self):
        pass

    async def end_long_transaction(self):
        self._long_txn_start = 0

    async def begin_transaction(self):
        if not self._changeset:
            self._changeset = changeset.ChangeSet()
        in_txn = self.in_long_transaction
        self._long_txn_start += 1
        if not in_txn:
            await self.start_long_transaction()

    async def abort(self):
        await self.end_transaction()


    async def really_commit(self):
        pass

    async def commit(self):
        if self.in_outer_transaction():
            await self.really_commit()
            self.end_long_transaction()
        self.close_current_transaction()

# Basic CRUD

    async def insert(self, kind, **spec):
        creator = base.kind_map[kind]
        coll = getattr(self.collections, kind)
        data = creator(**spec)
        return await self.meta_insert(data)

    async def upsert(self, class_name, data):
        the_id = data.get("id")
        m_class = self.metaclass_named(class_name)
        m_id = m_class.id
        extension = await self.extension(m_id)
        if the_id and m_id == oid.oid_class(the_id):
            await extension.replace(data)
        else:
            data.pop("id", None)
            await self.create_instance_of(class_name, **data)

    async def modify(self, kind, an_id, mods):
        coll = getattr(self.collections, kind)
        return await self.meta_modify(kind, an_id, **mods)

    async def delete(self, kind, an_id):
        coll = getattr(self.collections, kind)
        return await self.meta_delete(kind, an_id)

    async def add_class(self, **spec):
        return await self.insert("classes", **spec)

    async def modify_class(self, cls_id, **mods):
        return await self.modify("classes", cls_id, mods)

    async def delete_class(self, clsid):
        return await self.delete("classes", clsid)  

    async def add_attribute(self, **spec):
        return await self.insert("attributes", **spec)

    async def modify_attribute(self, attr_id, **mods):
        return await self.modify("attributes", attr_id, mods)

    async def delete_attribute(self, attrid):
        return await self.delete("attributes", attrid)

    async def add_role(self, **spec):
        return await self.insert("roles", **spec)

    async def modify_role(self, role_id, **mods):
        return await self.modify("roles", role_id, mods)

    async def add_role(self, **spec):
        return await self.insert("roles", **spec)

    async def modify_role(self, role_id, **mods):
        return await self.modify("roles", role_id, mods)

    async def delete_role(self, role_id):
        return await self.delete("roles", role_id)

    async def add_tag(self, **spec):
        return await self.insert("tags", **spec)

    async def modify_tag(self, tag_id, **mods):
        return await self.modify("tags", tag_id, mods)

    async def delete_tag(self, tag_id):
        return await self.delete("tags", tag_id)

    async def add_group(self, **spec):
        return await self.insert("groups", **spec)

    async def modify_group(self, group_id, **mods):
        return await self.modify("groups", group_id, mods)

    async def delete_group(self, group_id):
        return await self.delete("groups", group_id)

    async def add_object(self, obj):
        return await self.meta_insert(obj)

    async def modify_object(self, uuid, mods):
        return await self.meta_modify("objects", uuid, **mods)

    async def delete_object(self, uuid):
        return await self.meta_delete("objects", uuid)

    async def add_query(self, **spec):
        return await self.insert("queries", **spec)

    async def modify_query(self, query_id, **mods):
        return await self.modify("queries", query_id, mods)

    async def delete_query(self, query_id):
        return await self.delete("queries", query_id)   
    
    
