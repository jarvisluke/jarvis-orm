import sqlite3

from .model import Table
from .util import get_name, get_fields, get_primary_key


class SQLiteAdapter:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        cur = con.cursor()
        cur.execute("PRAGMA foreign_keys = ON")
        cur.close()
        
    # Wrapper methods
        
    def execute(self, query: str) -> None:
        cur = self.con.cursor()
        cur.execute(query)
        cur.close()
        
    def fetch(self, query: str) -> tuple | None:
        cur = self.con.cursor()
        cur.execute(query)
        result = cur.fetchone()
        cur.close()
        return result
    
    # DDL methods
    
    def create_table(self, table: type[Table]) -> None:
        keys = get_fields(table).keys()
        self.execute(f"CREATE TABLE {get_name(table)} ({', '.join(keys)})")
        
    def drop_table(self, table: type[Table]) -> None:
        self.execute(f"DROP TABLE [IF EXISTS] {get_name(table)}")
        
    # DML methods
    
    def insert(self, item: Table) -> None:
        fields = item.get_fields()
        self.execute(f"INSERT INTO {get_name(item.__class__)} ({', '.join(fields.keys())}) VALUES ({', '.join(fields.values())})",)
        self.con.commit()
    
    def delete(self, table: type[Table], pk) -> None:
        key = get_primary_key(table).keys()[0]
        self.execute(f"DELETE FROM {get_name(table)} WHERE {key} = {pk}")
    
    def select(self, table: type[Table], pk) -> tuple | None:
        key = get_primary_key(table).keys()[0]
        return self.fetch(f"SELECT * FROM {get_name(table)} WHERE {key} = {pk}")