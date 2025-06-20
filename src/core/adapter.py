import sqlite3
try:
    import psycopg2
except ImportError:
    psycopg2 = None

from .model import Affinity, Table
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
        
    def _execute(self, query: str, params: tuple = ()) -> None:
        cur = self.con.cursor()
        cur.execute(query, params)
        cur.close()
        
    def _fetch(self, query: str, params: tuple = ()) -> tuple | None:
        cur = self.con.cursor()
        cur.execute(query, params)
        result = cur.fetchone()
        cur.close()
        return result
    
    def _format_value(self, field) -> any:
        """Return the raw value instead of formatting as string"""
        return field.value
    
    # DDL methods
    
    def create_table(self, table: type[Table]) -> None:
        fields = []
        for k, v in get_fields(table).items():
            options = v.get_options()
            format = k
            
            # Adds affinity
            match v.affinity:
                case Affinity.TEXT:
                    format += " TEXT"
                case Affinity.INTEGER:
                    format += " INTEGER"
                case Affinity.REAL:
                    format += " REAL"
                case Affinity.BLOB:
                    format += " BLOB"
                    
            # Adds options
            if "not_null" in options:
                format += " NOT NULL"
            if "unique" in options:
                format += " UNIQUE"
            if "primary_key" in options:
                format += " PRIMARY KEY"
            fields.append(format)
            
            # Adds foreign key statement
            if "foreign_key" in options:
                pk_field = next(iter(get_primary_key(v.foreign_key)))
                fk_format = f"FOREIGN KEY ({k}) REFERENCES {get_name(v.foreign_key)} ({pk_field})"
                fk_format += " ON UPDATE " + v.on_update.value
                fk_format += " ON DELETE " + v.on_delete.value
                fields.append(fk_format)
                
        query = f"CREATE TABLE {get_name(table)} ({', '.join(fields)})"
        self._execute(query)
        
    def drop_table(self, table: type[Table]) -> None:
        # Validate the table name first
        table_name = get_name(table)
        if not table_name.replace('_', '').isalnum():
            raise ValueError("Invalid table name")
        query = f"DROP TABLE {table_name}"
        self._execute(query)
        
    # DML methods
    
    def insert(self, item: Table) -> None:
        # Get fields and values
        fields_dict = item.get_fields()
        field_names = list(fields_dict.keys())
        field_values = [self._format_value(v) for v in fields_dict.values()]
        
        # Build parameterized query
        table_name = get_name(item.__class__)
        placeholders = ', '.join(['?'] * len(field_names))
        query = f"INSERT INTO {table_name} ({', '.join(field_names)}) VALUES ({placeholders})"
        
        self._execute(query, tuple(field_values))
        self.con.commit()
    
    def update(self, item: Table) -> None:
        # Get fields and primary key
        fields_dict = item.get_fields()
        pk_dict = item.get_primary_key()
        
        # Separate field names and values for SET clause
        field_names = list(fields_dict.keys())
        field_values = [self._format_value(v) for v in fields_dict.values()]
        
        # Get primary key name and value
        pk_name = next(iter(pk_dict.keys()))
        pk_value = self._format_value(next(iter(pk_dict.values())))
        
        # Build parameterized query
        table_name = get_name(item.__class__)
        set_clauses = ', '.join([f"{name} = ?" for name in field_names])
        query = f"UPDATE {table_name} SET {set_clauses} WHERE {pk_name} = ?"
        
        # Combine field values and primary key value for parameters
        params = tuple(field_values + [pk_value])
        self._execute(query, params)
        self.con.commit()
    
    def delete(self, table: type[Table], pk) -> None:
        key = next(iter(get_primary_key(table).keys()))
        table_name = get_name(table)
        query = f"DELETE FROM {table_name} WHERE {key} = ?"
        self._execute(query, (pk,))
        self.con.commit()
    
    def select(self, table: type[Table], pk) -> tuple | None:
        key = next(iter(get_primary_key(table).keys()))
        table_name = get_name(table)
        query = f"SELECT * FROM {table_name} WHERE {key} = ?"
        return self._fetch(query, (pk,))
    
    # Additional query generation methods (for backward compatibility if needed)
    
    def get_create_table_query(self, table: type[Table]) -> str:
        """Generate CREATE TABLE query string (for debugging/logging purposes only)"""
        fields = []
        for k, v in get_fields(table).items():
            options = v.get_options()
            format = k
            
            # Adds affinity
            match v.affinity:
                case Affinity.TEXT:
                    format += " TEXT"
                case Affinity.INTEGER:
                    format += " INTEGER"
                case Affinity.REAL:
                    format += " REAL"
                case Affinity.BLOB:
                    format += " BLOB"
                    
            # Adds options
            if "not_null" in options:
                format += " NOT NULL"
            if "unique" in options:
                format += " UNIQUE"
            if "primary_key" in options:
                format += " PRIMARY KEY"
            fields.append(format)
            
            # Adds foreign key statement
            if "foreign_key" in options:
                pk_field = next(iter(get_primary_key(v.foreign_key)))
                fk_format = f"FOREIGN KEY ({k}) REFERENCES {get_name(v.foreign_key)} ({pk_field})"
                fk_format += " ON UPDATE " + v.on_update.value
                fk_format += " ON DELETE " + v.on_delete.value
                fields.append(fk_format)
                
        return f"CREATE TABLE {get_name(table)} ({', '.join(fields)})"


class PostgreSQLAdapter:
    def __init__(self, con) -> None:
        if psycopg2 is None:
            raise ImportError("psycopg2 is required for PostgreSQL support. Install it with: pip install psycopg2")
        self.con = con
        
    # Wrapper methods
        
    def _execute(self, query: str, params: tuple = ()) -> None:
        cur = self.con.cursor()
        cur.execute(query, params)
        cur.close()
        
    def _fetch(self, query: str, params: tuple = ()) -> tuple | None:
        cur = self.con.cursor()
        cur.execute(query, params)
        result = cur.fetchone()
        cur.close()
        return result
    
    def _format_value(self, field) -> any:
        """Return the raw value instead of formatting as string"""
        return field.value
    
    # DDL methods
    
    def create_table(self, table: type[Table]) -> None:
        fields = []
        for k, v in get_fields(table).items():
            options = v.get_options()
            format = k
            
            # Adds PostgreSQL data types
            match v.affinity:
                case Affinity.TEXT:
                    format += " TEXT"
                case Affinity.INTEGER:
                    if "primary_key" in options:
                        format += " SERIAL"  # Use SERIAL for auto-incrementing primary keys
                    else:
                        format += " INTEGER"
                case Affinity.REAL:
                    format += " REAL"
                case Affinity.BLOB:
                    format += " BYTEA"  # PostgreSQL uses BYTEA for binary data
                    
            # Adds options
            if "not_null" in options:
                format += " NOT NULL"
            if "unique" in options:
                format += " UNIQUE"
            if "primary_key" in options and v.affinity != Affinity.INTEGER:
                format += " PRIMARY KEY"
            elif "primary_key" in options and v.affinity == Affinity.INTEGER:
                format += " PRIMARY KEY"  # SERIAL already implies NOT NULL
            fields.append(format)
            
            # Adds foreign key statement
            if "foreign_key" in options:
                pk_field = next(iter(get_primary_key(v.foreign_key)))
                fk_format = f"FOREIGN KEY ({k}) REFERENCES {get_name(v.foreign_key)} ({pk_field})"
                fk_format += " ON UPDATE " + v.on_update.value
                fk_format += " ON DELETE " + v.on_delete.value
                fields.append(fk_format)
                
        query = f"CREATE TABLE IF NOT EXISTS {get_name(table)} ({', '.join(fields)})"
        self._execute(query)
        self.con.commit()
        
    def drop_table(self, table: type[Table]) -> None:
        # Validate the table name first
        table_name = get_name(table)
        if not table_name.replace('_', '').isalnum():
            raise ValueError("Invalid table name")
        query = f"DROP TABLE IF EXISTS {table_name} CASCADE"  # CASCADE to drop dependent objects
        self._execute(query)
        self.con.commit()
        
    # DML methods
    
    def insert(self, item: Table) -> None:
        # Get fields and values
        fields_dict = item.get_fields()
        field_names = list(fields_dict.keys())
        field_values = [self._format_value(v) for v in fields_dict.values()]
        
        # Build parameterized query with PostgreSQL placeholders
        table_name = get_name(item.__class__)
        placeholders = ', '.join(['%s'] * len(field_names))  # PostgreSQL uses %s
        query = f"INSERT INTO {table_name} ({', '.join(field_names)}) VALUES ({placeholders})"
        
        self._execute(query, tuple(field_values))
        self.con.commit()
    
    def update(self, item: Table) -> None:
        # Get fields and primary key
        fields_dict = item.get_fields()
        pk_dict = item.get_primary_key()
        
        # Separate field names and values for SET clause
        field_names = list(fields_dict.keys())
        field_values = [self._format_value(v) for v in fields_dict.values()]
        
        # Get primary key name and value
        pk_name = next(iter(pk_dict.keys()))
        pk_value = self._format_value(next(iter(pk_dict.values())))
        
        # Build parameterized query with PostgreSQL placeholders
        table_name = get_name(item.__class__)
        set_clauses = ', '.join([f"{name} = %s" for name in field_names])  # PostgreSQL uses %s
        query = f"UPDATE {table_name} SET {set_clauses} WHERE {pk_name} = %s"
        
        # Combine field values and primary key value for parameters
        params = tuple(field_values + [pk_value])
        self._execute(query, params)
        self.con.commit()
    
    def delete(self, table: type[Table], pk) -> None:
        key = next(iter(get_primary_key(table).keys()))
        table_name = get_name(table)
        query = f"DELETE FROM {table_name} WHERE {key} = %s"  # PostgreSQL uses %s
        self._execute(query, (pk,))
        self.con.commit()
    
    def select(self, table: type[Table], pk) -> tuple | None:
        key = next(iter(get_primary_key(table).keys()))
        table_name = get_name(table)
        query = f"SELECT * FROM {table_name} WHERE {key} = %s"  # PostgreSQL uses %s
        return self._fetch(query, (pk,))
    
    # Additional query generation methods (for backward compatibility if needed)
    
    def get_create_table_query(self, table: type[Table]) -> str:
        """Generate CREATE TABLE query string (for debugging/logging purposes only)"""
        fields = []
        for k, v in get_fields(table).items():
            options = v.get_options()
            format = k
            
            # Adds PostgreSQL data types
            match v.affinity:
                case Affinity.TEXT:
                    format += " TEXT"
                case Affinity.INTEGER:
                    if "primary_key" in options:
                        format += " SERIAL"
                    else:
                        format += " INTEGER"
                case Affinity.REAL:
                    format += " REAL"
                case Affinity.BLOB:
                    format += " BYTEA"
                    
            # Adds options
            if "not_null" in options:
                format += " NOT NULL"
            if "unique" in options:
                format += " UNIQUE"
            if "primary_key" in options and v.affinity != Affinity.INTEGER:
                format += " PRIMARY KEY"
            elif "primary_key" in options and v.affinity == Affinity.INTEGER:
                format += " PRIMARY KEY"
            fields.append(format)
            
            # Adds foreign key statement
            if "foreign_key" in options:
                pk_field = next(iter(get_primary_key(v.foreign_key)))
                fk_format = f"FOREIGN KEY ({k}) REFERENCES {get_name(v.foreign_key)} ({pk_field})"
                fk_format += " ON UPDATE " + v.on_update.value
                fk_format += " ON DELETE " + v.on_delete.value
                fields.append(fk_format)
                
        return f"CREATE TABLE IF NOT EXISTS {get_name(table)} ({', '.join(fields)})"