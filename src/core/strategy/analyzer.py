from typing import Dict, List, Set, Type, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import sqlite3
import logging

try:
    import psycopg2
except ImportError:
    psycopg2 = None

from ..model import Table, Field, Affinity
from ..util import get_name, get_fields


class SchemaChangeType(Enum):
    """Types of schema changes"""
    TABLE_CREATE = "TABLE_CREATE"
    TABLE_DROP = "TABLE_DROP"
    COLUMN_ADD = "COLUMN_ADD"
    COLUMN_DROP = "COLUMN_DROP"
    COLUMN_MODIFY = "COLUMN_MODIFY"


@dataclass
class SchemaChange:
    """Represents a single schema change"""
    change_type: SchemaChangeType
    table_name: str
    details: Dict[str, any]
    
    def __str__(self):
        return f"{self.change_type.value}: {self.table_name} - {self.details}"


class SchemaAnalyzer:
    """
    Analyzes database schema and compares with model definitions.
    """
    
    def __init__(self, connection, logger: Optional[logging.Logger] = None):
        self.connection = connection
        self.logger = logger
        
        # Determine database type
        if isinstance(connection, sqlite3.Connection):
            self.db_type = "sqlite"
        elif psycopg2 and isinstance(connection, psycopg2.extensions.connection):
            self.db_type = "postgresql"
        else:
            raise ValueError(f"Unsupported connection type: {type(connection)}")
            
    def get_existing_tables(self) -> Set[str]:
        """Get set of existing table names in the database"""
        cursor = self.connection.cursor()
        
        try:
            if self.db_type == "sqlite":
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name NOT LIKE 'sqlite_%'"
                )
            else:  # postgresql
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public'"
                )
                
            return {row[0].lower() for row in cursor.fetchall()}
            
        finally:
            cursor.close()
            
    def get_table_columns(self, table_name: str) -> Dict[str, Dict[str, any]]:
        """Get column information for a table"""
        cursor = self.connection.cursor()
        columns = {}
        
        try:
            if self.db_type == "sqlite":
                cursor.execute(f"PRAGMA table_info({table_name})")
                for row in cursor.fetchall():
                    columns[row[1]] = {
                        'type': row[2],
                        'not_null': bool(row[3]),
                        'default': row[4],
                        'primary_key': bool(row[5])
                    }
            else:  # postgresql
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_default,
                           CASE WHEN constraint_type = 'PRIMARY KEY' THEN true ELSE false END as is_primary
                    FROM information_schema.columns
                    LEFT JOIN (
                        SELECT kcu.column_name, tc.constraint_type
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
                    ) pk ON columns.column_name = pk.column_name
                    WHERE table_name = %s
                """, (table_name, table_name))
                
                for row in cursor.fetchall():
                    columns[row[0]] = {
                        'type': row[1],
                        'not_null': row[2] == 'NO',
                        'default': row[3],
                        'primary_key': row[4] or False
                    }
                    
            return columns
            
        finally:
            cursor.close()
            
    def compare_table(self, table: Type[Table]) -> List[SchemaChange]:
        """Compare a model table definition with the database schema"""
        changes = []
        table_name = get_name(table)
        
        # Check if table exists
        existing_tables = self.get_existing_tables()
        if table_name not in existing_tables:
            changes.append(SchemaChange(
                SchemaChangeType.TABLE_CREATE,
                table_name,
                {'fields': list(get_fields(table).keys())}
            ))
            return changes
            
        # Compare columns
        db_columns = self.get_table_columns(table_name)
        model_fields = get_fields(table)
        
        db_col_names = set(db_columns.keys())
        model_col_names = set(model_fields.keys())
        
        # Find added columns
        for col in model_col_names - db_col_names:
            changes.append(SchemaChange(
                SchemaChangeType.COLUMN_ADD,
                table_name,
                {'column': col, 'field': model_fields[col]}
            ))
            
        # Find dropped columns
        for col in db_col_names - model_col_names:
            changes.append(SchemaChange(
                SchemaChangeType.COLUMN_DROP,
                table_name,
                {'column': col}
            ))
            
        # Find modified columns
        for col in db_col_names & model_col_names:
            # This is simplified - real comparison would need type mapping
            model_field = model_fields[col]
            db_info = db_columns[col]
            
            if model_field.not_null != db_info['not_null']:
                changes.append(SchemaChange(
                    SchemaChangeType.COLUMN_MODIFY,
                    table_name,
                    {
                        'column': col,
                        'change': 'not_null',
                        'from': db_info['not_null'],
                        'to': model_field.not_null
                    }
                ))
                
        return changes
        
    def analyze_schema(self, tables: List[Type[Table]]) -> Dict[str, List[SchemaChange]]:
        """
        Analyze schema differences for multiple tables.
        Returns dict mapping table names to their changes.
        """
        all_changes = {}
        
        for table in tables:
            table_name = get_name(table)
            changes = self.compare_table(table)
            if changes:
                all_changes[table_name] = changes
                
        # Find tables to drop (exist in DB but not in models)
        existing_tables = self.get_existing_tables()
        model_tables = {get_name(t) for t in tables}
        
        for table_name in existing_tables - model_tables:
            all_changes[table_name] = [SchemaChange(
                SchemaChangeType.TABLE_DROP,
                table_name,
                {}
            )]
            
        if self.logger:
            total_changes = sum(len(changes) for changes in all_changes.values())
            self.logger.info(f"Schema analysis found {total_changes} changes in {len(all_changes)} tables")
            
        return all_changes
        
    def generate_migration_sql(self, changes: List[SchemaChange]) -> List[str]:
        """
        Generate SQL statements for schema changes.
        Note: This is simplified and doesn't handle all cases.
        """
        sql_statements = []
        
        for change in changes:
            if change.change_type == SchemaChangeType.TABLE_CREATE:
                # This would need the full table definition
                sql_statements.append(f"-- CREATE TABLE {change.table_name}")
                
            elif change.change_type == SchemaChangeType.TABLE_DROP:
                sql_statements.append(f"DROP TABLE {change.table_name};")
                
            elif change.change_type == SchemaChangeType.COLUMN_ADD:
                # This would need the column definition
                sql_statements.append(
                    f"ALTER TABLE {change.table_name} ADD COLUMN {change.details['column']};"
                )
                
            elif change.change_type == SchemaChangeType.COLUMN_DROP:
                sql_statements.append(
                    f"ALTER TABLE {change.table_name} DROP COLUMN {change.details['column']};"
                )
                
        return sql_statements