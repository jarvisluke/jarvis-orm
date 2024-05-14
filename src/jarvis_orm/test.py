from sqlite3 import Connection
from core.model import *
from core.engine import Engine

# Establish connection to database
con = Connection("test.db")
orm = Engine(con)

# Define our models
class Company(Table):
    id = TextField(primary_key=True)
    name = TextField(unique=True)


class Person(Table):
    id = TextField(primary_key=True)
    name = TextField(unique=True)
    company = TextField(foreign_key=Company)

# Build our objects
dev = Company(id="300", name="Dev Co")
luke = Person(id="100", name="Luke", company=dev)

orm.create(Person)