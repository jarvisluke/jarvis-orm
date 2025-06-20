from sqlite3 import Error
import sqlite3

try:
    import psycopg2
    import psycopg2.extensions
except ImportError:
    psycopg2 = None

from .adapter import SQLiteAdapter, PostgreSQLAdapter
from .exceptions import UnsupportedDatabase
from .model import Table
from .util import get_fields


class Engine:
    def __init__(self, con) -> None:
        self.db = None
        
        # Match connection to database
        if isinstance(con, sqlite3.Connection):
            self.db = SQLiteAdapter(con)
        elif psycopg2 and isinstance(con, psycopg2.extensions.connection):
            self.db = PostgreSQLAdapter(con)
        else:
            # Determine connection type for error message
            con_type = type(con).__name__
            supported = ["sqlite3.Connection"]
            if psycopg2:
                supported.append("psycopg2.extensions.connection")
            
            raise UnsupportedDatabase(
                f"Unsupported database connection type: {con_type}. "
                f"Supported types are: {', '.join(supported)}"
            )
        
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