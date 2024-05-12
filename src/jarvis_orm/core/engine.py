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
        values = self.db.fetch(table, pk)
        keys = tuple(get_fields(table))
        kwargs = {keys[i]: values[i] for i in range(len(keys))}
        
        # Create new table object
        return table(**kwargs)
        
    def save(self, item: Table) -> None:
        cur = self.con.cursor()
        pk = str(item.get_primary_key())
        name = item.__class__.__name__.lower()
        
        try:
            # Checks if item exists
            cur.execute(f"SELECT * FROM {name} WHERE {pk+' = '+str((getattr(item, pk).value))};",)
            
            # Updates or inserts row
            if cur.fetchone():
                cur.execute(item.get_update_string())
            else:
                cur.execute(item.get_insert_string())
        except Error as e:
            print(e)
                
        cur.close()
        self.con.commit()
        
    def delete(self, item: Table) -> None:
        cur = self.con.cursor()
        pk = str(item.get_primary_key())
        name = item.__class__.__name__.lower()
        
        try:
            # Checks if item exists
            cur.execute(f"DELETE FROM {name} WHERE {pk+' = '+str(getattr(item, pk).value)};",)
            
            # Updates or inserts row
            if cur.fetchone():
                cur.execute(item.get_update_string())
            else:
                cur.execute(item.get_insert_string())
        except Error as e:
            print(e)
                
        cur.close()
        self.con.commit()