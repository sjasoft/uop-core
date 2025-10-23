__author__ = "samantha"

from sjasoft.uop import testing
from sjasoft.uopmeta.schemas import meta
from sjasoft.uopmeta.schemas.predefined import pkm_schema


schemas = (pkm_schema,)


context: testing.TestContext = None


def set_context(**the_credentials):
    global context
    context = testing.TestContext.fresh_context(**the_credentials)


async def complete_context():
    global context
    await context.service_and_db_class()


def check_extensions():
    global context
    pass


async def test_db():
    """
    This is the main test of UOP db_interface.  To use it for a particular interface
    first use set_context to set the inferface to use. Then call/await this function.

    :return:
    """
    global context, schemas
    await context.complete_context(schemas=schemas)
    random_data = context.dataset()
    db_tagged = context.get_db_method("tagged")
    db_grouped = context.get_db_method("grouped")
    db_related = context.get_db_method("related")
    insert_and_check(random_data, db_tagged, db_grouped, db_related)
    modify_and_check(random_data, db_tagged, db_grouped, db_related)
    delete_and_check(random_data, db_tagged, db_grouped, db_related)
