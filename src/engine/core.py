import sqlite3
from typing import Type

from ..schema.model import Table, Field


class Engine:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        
    def get(self, table: Type[Table], pk: str) -> Table:
        cur = self.con.cursor()
        cur.execute(f"SELECT * FROM {table.__name__.lower()} WHERE {table.get_primary_key_cls()+' = '+pk}")
        
        # Create dict of keys: values
        values = cur.fetchone()
        keys = tuple(table.get_fields_cls())
        kwargs = {keys[i]: values[i] for i in range(len(keys))}
        
        # Create new table object
        obj = table(**kwargs)
        
        cur.close()
        return obj
        
    def create(self, table: Type[Table]):
        cur = self.con.cursor()
        print(table.get_create_query())
        cur.execute(table.get_create_query())
        cur.close()
        self.con.commit()
        
    def save(self, item: Table) -> None:
        cur = self.con.cursor()
        pk = item.get_primary_key()
        name = item.__class__.__name__.lower()
        
        # Checks if item exists
        cur.execute(f"SELECT * FROM {name} WHERE {pk+' = '+getattr(item, pk).value};",)
        
        # Updates or inserts row
        if cur.fetchone():
            cur.execute(item.get_update_query())
        else:
            cur.execute(item.get_insert_query())
            
        cur.close()
        self.con.commit()