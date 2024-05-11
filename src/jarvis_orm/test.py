from core.model import Table
from core.model import IntegerField, TextField


class Person(Table):
    id = TextField(primary_key=True)
    name = TextField(unique=True)

luke = Person(id="100", name="Luke")