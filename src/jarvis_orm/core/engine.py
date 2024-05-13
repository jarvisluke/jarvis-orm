from sqlite3 import Error
import sqlite3

from .adapter import SQLiteAdapter
from .model import Table
from .util import get_fields


class Engine:
    def __init__(self, con) -> None:
        self.db = None
        # Match connection to database
        match con.__class__:
            # sqlite
            case sqlite3.Connection:
                self.db = SQLiteAdapter(con)
        
    def create(self, table: type[Table]):
        self.db.create_table(table)
        
    def delete(self, table: type[Table]):
        self.db.drop_table(table)
        
    def get(self, table: type[Table], pk) -> Table:
        values = self.db.select(table, pk)
        # If table with pk in exists
        if values:
            keys = tuple(get_fields(table))
            kwargs = {keys[i]: values[i] for i in range(len(keys))}
            # Return a new table object
            return table(**kwargs)
        else:
            return None
        
    def save(self, item: Table) -> None:
        # Update item if it already exists
        if self.get(item.__class__, next(iter(item.get_primary_key().values()))):
            self.db.update(item)
        # Insert item if it does not exist
        else:
            self.db.insert(item)