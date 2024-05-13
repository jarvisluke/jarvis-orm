from sqlite3 import Connection
from core.model import *
from core.engine import Engine

# Establish connection to database
con = Connection("test.db")
orm = Engine(con)

# Define our model
class Person(Table):
    id = TextField(primary_key=True)
    name = TextField(unique=True)

# Build our objects
luke = Person(id="100", name="Luke")

orm.save(luke)