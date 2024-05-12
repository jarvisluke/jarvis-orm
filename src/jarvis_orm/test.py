from core.model import *


class Person(Table):
    id = TextField(primary_key=True)
    name = TextField(unique=True)

luke = Person(id="100", name="Luke")

pk = luke.get_primary_key()
print(get_name(Person))