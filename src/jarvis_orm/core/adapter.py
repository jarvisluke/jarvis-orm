import sqlite3
from typing import Type
from .model import Table


class SQLiteAdapter:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        cur = con.cursor()
        cur.execute("PRAGMA foreign_keys = ON")
        cur.close()
        
    def execute(self, query: str) -> None:
        cur = self.con.cursor()
        cur.execute(query)
        cur.close()
        
    '''
    Data definition language (DDL): Commands that define the database schema
    '''
        
    def create_table(self, table: Type[Table]) -> None:
        self.execute(f"CREATE TABLE {table.get_name()}({', '.join(table.get_fields())})")
        
    def drop_table(self, table: Type[Table]) -> None:
        self.execute(f"DROP TABLE [IF EXISTS] {table.get_name()}")
        
    '''
    Data manipulation language (DML): Commands that manipulate database data
        Transactions require call to self.con.commit()
    '''
    
    def insert(self, item: Table) -> None:
        self.execute(f"INSERT INTO {item.__class__.get_name()} VALUES({'?' * len(item.__class__.get_fields())})",)
        self.con.commit()
    
    def delete(self, item: Table) -> None:
        self.execute(f"DELETE FROM {item.__class__.get_name()} WHERE {item.get_primary_key()}")
    
    def select(self, row: Table) -> None:
        pass

    