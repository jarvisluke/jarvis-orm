import sqlite3

from ..schema.model import Table, Field


class Engine:
    def __init__(self, con) -> None:
        self.con = con
        
    def get(self, table, pk) -> Table:
        cur = self.con.cursor()
        cur.execute(
            "SELECT * FROM ? WHERE ?);",
            (table.__name__, table.get_primary_key_cls())
            )
        return
        #obj = table.build(*obj)
        #return obj
        
    def create(self, item):
        cur = self.con.cursor()
        print(item.get_create_query())
        cur.execute(item.get_create_query())
       
    def save(self, item: Table):
        cur = self.con.cursor()
        pk = item.get_primary_key()
        name = item.__class__.__name__.lower()
        
        # Checks if item exists
        cur.execute(
            f"SELECT * FROM {name} WHERE {pk+" = "+getattr(item, pk).value};",
            )
        
        # Updates or inserts row
        if cur.fetchone()[0]:
            print(item.get_update_string())
            cur.execute(*item.get_update_string())
        else:
            print(item.get_insert_query())
            cur.execute(item.get_insert_query())
        cur.close()
        self.con.commit()