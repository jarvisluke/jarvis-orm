from sqlite3 import Connection, Error
from typing import Type

from ..schema.model import Table, Field


class Engine:
    def __init__(self, con: Connection, foreign_keys=True) -> None:
        self.con = con
        if foreign_keys:
            cur = self.con.cursor()
            cur.execute("PRAGMA foreign_keys = ON")
            cur.close()
        
    def get(self, table: Type[Table], pk: str | Field) -> Table:
        if isinstance(pk, Field):
            pk = pk.value
        cur = self.con.cursor()
        
        
        try:
            cur.execute(f"SELECT * FROM {table.__name__.lower()} WHERE {table.get_primary_key_cls()+' = '+pk}")
        except Error as e:
            print(e)
        
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
        try:
            cur.execute(table.get_create_query())
        except Error as e:
            print(e)
            
        cur.close()
        self.con.commit()
        
    def save(self, item: Table) -> None:
        cur = self.con.cursor()
        pk = item.get_primary_key()
        name = item.__class__.__name__.lower()
        
        try:
            # Checks if item exists
            cur.execute(f"SELECT * FROM {name} WHERE {pk+' = '+getattr(item, pk).value};",)
            
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
        pk = item.get_primary_key()
        name = item.__class__.__name__.lower()
        
        try:
            # Checks if item exists
            cur.execute(f"DELETE FROM {name} WHERE {pk+' = '+getattr(item, pk).value};",)
            
            # Updates or inserts row
            if cur.fetchone():
                cur.execute(item.get_update_string())
            else:
                cur.execute(item.get_insert_string())
        except Error as e:
            print(e)
                
        cur.close()
        self.con.commit()