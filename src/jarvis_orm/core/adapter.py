import sqlite3

from .model import Table
from .util import get_name, get_fields, get_primary_key

'''
All adapters must be "duck-typed", i.e.: they must all have the same signature for each DDL and DML method

sqlite3 is part of the Python standard libraries, all other adapters require an optional connector dependency
'''

class SQLiteAdapter:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        cur = con.cursor()
        cur.execute("PRAGMA foreign_keys = ON")
        cur.close()
        
    # Wrapper methods
        
    def _execute(self, query: str) -> None:
        cur = self.con.cursor()
        cur.execute(query)
        cur.close()
        
    def _fetch(self, query: str) -> tuple | None:
        cur = self.con.cursor()
        cur.execute(query)
        result = cur.fetchone()
        cur.close()
        return result
    
    def _format_value(self, field) -> str:
        if isinstance(field.value, str):
            return f"'{field.value}'"
        else:
            return str(field.value)
    
    # DDL methods
    
    def create_table(self, table: type[Table]) -> None:
        keys = get_fields(table).keys()
        self._execute(f"CREATE TABLE {get_name(table)} ({', '.join(keys)})")
        
    def drop_table(self, table: type[Table]) -> None:
        self._execute(f"DROP TABLE {get_name(table)}")
        
    # DML methods
    
    def insert(self, item: Table) -> None:
        # TODO: ADD MORE INFORMATION TO FIELDS PART OF QUERY
        fields = item.get_fields()
        self._execute(f"INSERT INTO {get_name(item.__class__)} ({', '.join([str(k) for k in fields.keys()])}) VALUES ({', '.join([self._format_value(v) for v in fields.values()])})")
        self.con.commit()
    
    def update(self, item: Table) -> None:
        fields = ", ".join([k + " = " + self._format_value(v) for k, v in item.get_fields().items()])  # format: field1 = value1, field2 = value2...
        pk = item.get_primary_key()
        self._execute(f"UPDATE {get_name(item.__class__)} SET {fields} WHERE {next(iter(pk.keys()))} = {next(iter(pk.values()))}")
        
    def delete(self, table: type[Table], pk) -> None:
        key = next(iter(get_primary_key(table).keys()))
        self._execute(f"DELETE FROM {get_name(table)} WHERE {key} = {pk}")
    
    def select(self, table: type[Table], pk) -> tuple | None:
        key = next(iter(get_primary_key(table).keys()))
        return self._fetch(f"SELECT * FROM {get_name(table)} WHERE {key} = {pk}")