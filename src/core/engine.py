from sqlite3 import Error
import sqlite3
import logging
from typing import Optional, List, Type, Dict, Any, Callable

try:
    import psycopg2
    import psycopg2.extensions
except ImportError:
    psycopg2 = None

from .adapter import SQLiteAdapter, PostgreSQLAdapter
from .exceptions import UnsupportedDatabase
from .model import Table
from .util import get_fields
from .strategy import DDLStrategy, DependencyGraph, CircularDependencyError, DDLOperation


class Engine:
    def __init__(self, con, logger: Optional[logging.Logger] = None) -> None:
        self.db = None
        self.logger = None
        self.connection = con  # Store original connection for strategy layer
        
        # Strategy components
        self.ddl_strategy: Optional[DDLStrategy] = None
        self.dependency_graph = DependencyGraph()
        self._registered_tables: List[Type[Table]] = []
        
        # Validate logger type if provided
        if logger is not None:
            if not isinstance(logger, logging.Logger):
                raise TypeError(
                    f"Logger must be an instance of logging.Logger, "
                    f"got {type(logger).__name__}"
                )
            self.logger = logger
            
            # Log engine initialization at INFO level
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"Initializing ORM Engine with connection type: {type(con).__name__}")
        
        # Match connection to database
        if isinstance(con, sqlite3.Connection):
            self.db = SQLiteAdapter(con, logger)
            db_type = "SQLite"
        elif psycopg2 and isinstance(con, psycopg2.extensions.connection):
            self.db = PostgreSQLAdapter(con, logger)
            db_type = "PostgreSQL"
        else:
            # Determine connection type for error message
            con_type = type(con).__name__
            supported = ["sqlite3.Connection"]
            if psycopg2:
                supported.append("psycopg2.extensions.connection")
            
            error_msg = (
                f"Unsupported database connection type: {con_type}. "
                f"Supported types are: {', '.join(supported)}"
            )
            
            if logger and logger.isEnabledFor(logging.ERROR):
                logger.error(f"Engine initialization failed: {error_msg}")
            
            raise UnsupportedDatabase(error_msg)
            
        # Initialize DDL strategy
        self.ddl_strategy = DDLStrategy(self.db, logger)
        
        if logger and logger.isEnabledFor(logging.INFO):
            logger.info(f"ORM Engine initialized successfully with {db_type} adapter and DDL strategy")

    # Table Registration and Management
    
    def register_table(self, table: Type[Table]) -> None:
        """Register a single table with the engine's dependency graph"""
        if table not in self._registered_tables:
            self._registered_tables.append(table)
            self.dependency_graph.add_table(table)
            self.ddl_strategy.register_tables([table])
            
            if self.logger and self.logger.isEnabledFor(logging.DEBUG):
                from .util import get_name
                self.logger.debug(f"Registered table: {get_name(table)}")
    
    def register_tables(self, tables: List[Type[Table]]) -> None:
        """Register multiple tables with the engine's dependency graph"""
        new_tables = [t for t in tables if t not in self._registered_tables]
        self._registered_tables.extend(new_tables)
        
        if new_tables:
            self.dependency_graph.add_tables(new_tables)
            self.ddl_strategy.register_tables(new_tables)
            
            if self.logger and self.logger.isEnabledFor(logging.INFO):
                self.logger.info(f"Registered {len(new_tables)} new tables (total: {len(self._registered_tables)})")
    
    def get_registered_tables(self) -> List[Type[Table]]:
        """Get list of all registered tables"""
        return self._registered_tables.copy()
    
    def clear_registered_tables(self) -> None:
        """Clear all registered tables"""
        self._registered_tables.clear()
        self.dependency_graph = DependencyGraph()
        self.ddl_strategy = DDLStrategy(self.db, self.logger)
        
        if self.logger:
            self.logger.info("Cleared all registered tables")

    # DDL Operations (Enhanced with Strategy)
    
    def create(self, table: Type[Table]) -> None:
        """Create a single table (legacy method for backward compatibility)"""
        if self.logger and self.logger.isEnabledFor(logging.INFO):
            self.logger.info(f"Engine.create() called for table: {table.__name__}")
        
        # Auto-register if not already registered
        if table not in self._registered_tables:
            self.register_table(table)
            
        self.db.create_table(table)
        
    def delete(self, table: Type[Table]) -> None:
        """Delete/drop a single table (legacy method for backward compatibility)"""
        if self.logger and self.logger.isEnabledFor(logging.INFO):
            self.logger.info(f"Engine.delete() called for table: {table.__name__}")
        self.db.drop_table(table)
    
    def create_all(self, tables: Optional[List[Type[Table]]] = None, 
                   stop_on_error: bool = True,
                   callback: Optional[Callable[[DDLOperation, bool, Optional[Exception]], None]] = None) -> Dict[str, Any]:
        """
        Create all registered tables or specified tables in dependency order.
        
        Args:
            tables: Optional list of specific tables to create. If None, creates all registered tables.
            stop_on_error: If True, stop on first error. If False, continue with remaining operations.
            callback: Optional callback function called after each operation (op, success, exception)
            
        Returns:
            Dictionary with execution results
        """
        if tables:
            # Register any unregistered tables
            self.register_tables(tables)
            # Create a temporary strategy for just these tables
            temp_strategy = DDLStrategy(self.db, self.logger)
            temp_strategy.register_tables(tables)
            result = temp_strategy.create_all(stop_on_error)
        else:
            # Create all registered tables
            if not self._registered_tables:
                if self.logger:
                    self.logger.warning("No tables registered for creation")
                return {'total': 0, 'successful': 0, 'failed': 0, 'errors': []}
            
            result = self.ddl_strategy.create_all(stop_on_error)
        
        if callback:
            # Note: callback integration would require modifying DDLStrategy.execute_operations
            # This is a placeholder for the interface
            pass
            
        return result
    
    def drop_all(self, tables: Optional[List[Type[Table]]] = None, 
                 stop_on_error: bool = True,
                 callback: Optional[Callable[[DDLOperation, bool, Optional[Exception]], None]] = None) -> Dict[str, Any]:
        """
        Drop all registered tables or specified tables in reverse dependency order.
        
        Args:
            tables: Optional list of specific tables to drop. If None, drops all registered tables.
            stop_on_error: If True, stop on first error. If False, continue with remaining operations.
            callback: Optional callback function called after each operation (op, success, exception)
            
        Returns:
            Dictionary with execution results
        """
        if tables:
            # Create a temporary strategy for just these tables
            temp_strategy = DDLStrategy(self.db, self.logger)
            temp_strategy.register_tables(tables)
            result = temp_strategy.drop_all(stop_on_error)
        else:
            # Drop all registered tables
            if not self._registered_tables:
                if self.logger:
                    self.logger.warning("No tables registered for deletion")
                return {'total': 0, 'successful': 0, 'failed': 0, 'errors': []}
            
            result = self.ddl_strategy.drop_all(stop_on_error)
        
        if callback:
            # Note: callback integration would require modifying DDLStrategy.execute_operations
            # This is a placeholder for the interface
            pass
            
        return result

    # Dependency Analysis Methods
    
    def get_creation_order(self, tables: Optional[List[Type[Table]]] = None) -> List[Type[Table]]:
        """
        Get the order in which tables should be created based on dependencies.
        
        Args:
            tables: Optional list of specific tables. If None, uses all registered tables.
            
        Returns:
            List of tables in creation order
        """
        if tables:
            temp_graph = DependencyGraph()
            temp_graph.add_tables(tables)
            return temp_graph.get_creation_order()
        else:
            return self.dependency_graph.get_creation_order()
    
    def get_deletion_order(self, tables: Optional[List[Type[Table]]] = None) -> List[Type[Table]]:
        """
        Get the order in which tables should be deleted based on dependencies.
        
        Args:
            tables: Optional list of specific tables. If None, uses all registered tables.
            
        Returns:
            List of tables in deletion order
        """
        if tables:
            temp_graph = DependencyGraph()
            temp_graph.add_tables(tables)
            return temp_graph.get_deletion_order()
        else:
            return self.dependency_graph.get_deletion_order()
    
    def get_table_dependencies(self, table: Type[Table]) -> List[Type[Table]]:
        """Get all tables that the given table depends on"""
        dependencies = self.dependency_graph.get_dependencies(table)
        return list(dependencies)
    
    def get_table_dependents(self, table: Type[Table]) -> List[Type[Table]]:
        """Get all tables that depend on the given table"""
        dependents = self.dependency_graph.get_dependents(table)
        return list(dependents)
    
    def get_dependency_levels(self) -> Dict[int, List[Type[Table]]]:
        """
        Group registered tables by dependency level.
        Level 0 has no dependencies, level 1 depends only on level 0, etc.
        Tables at the same level could potentially be processed in parallel.
        """
        return self.dependency_graph.get_dependency_levels()
    
    def visualize_dependencies(self) -> str:
        """Generate a text representation of the dependency graph for registered tables"""
        if not self._registered_tables:
            return "No tables registered"
        return self.dependency_graph.visualize()
    
    def validate_dependencies(self) -> List[str]:
        """
        Validate the dependency graph for potential issues.
        Returns list of warning/error messages.
        """
        issues = []
        
        try:
            # This will raise CircularDependencyError if there are cycles
            self.dependency_graph.get_creation_order()
        except CircularDependencyError as e:
            issues.append(f"Circular dependency detected: {e}")
        
        # Additional validations could be added here
        
        return issues

    # Planning Methods (for inspection without execution)
    
    def plan_create_all(self, tables: Optional[List[Type[Table]]] = None) -> List[DDLOperation]:
        """
        Plan creation of tables without executing the operations.
        Useful for inspection and debugging.
        """
        if tables:
            temp_strategy = DDLStrategy(self.db, self.logger)
            temp_strategy.register_tables(tables)
            return temp_strategy.plan_create_all()
        else:
            return self.ddl_strategy.plan_create_all()
    
    def plan_drop_all(self, tables: Optional[List[Type[Table]]] = None) -> List[DDLOperation]:
        """
        Plan deletion of tables without executing the operations.
        Useful for inspection and debugging.
        """
        if tables:
            temp_strategy = DDLStrategy(self.db, self.logger)
            temp_strategy.register_tables(tables)
            return temp_strategy.plan_drop_all()
        else:
            return self.ddl_strategy.plan_drop_all()

    # DML Operations (unchanged from original)
        
    def get(self, table: Type[Table], pk) -> Table:
        """Get a single record by primary key"""
        # Data operations logged at DEBUG
        if self.logger and self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Engine.get() called for table: {table.__name__}, pk: {pk}")
        
        values = self.db.select(table, pk)
        # If table with pk exists
        if values:
            keys = tuple(get_fields(table))
            kwargs = {keys[i]: values[i] for i in range(len(keys))}
            # Return a new table object
            result = table(**kwargs)
            if self.logger and self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"Engine.get() returned object for table: {table.__name__}, pk: {pk}")
            return result
        else:
            if self.logger and self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"Engine.get() returned None for table: {table.__name__}, pk: {pk}")
            return None
        
    def save(self, item: Table) -> None:
        """Save a record (insert if new, update if exists)"""
        pk_value = next(iter(item.get_primary_key().values())).value
        # Data operations logged at DEBUG
        if self.logger and self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Engine.save() called for table: {item.__class__.__name__}, pk: {pk_value}")
        
        # Update item if it already exists
        if self.get(item.__class__, pk_value):
            self.db.update(item)
        # Insert item if it does not exist
        else:
            self.db.insert(item)
    
    def insert(self, item: Table) -> None:
        """Insert a new record"""
        self.db.insert(item)
    
    def update(self, item: Table) -> None:
        """Update an existing record"""
        self.db.update(item)
    
    def remove(self, table: Type[Table], pk) -> None:
        """Delete a record by primary key"""
        self.db.delete(table, pk)

    # Utility Methods
    
    def get_adapter(self):
        """Get the underlying database adapter (useful for advanced operations)"""
        return self.db
    
    def get_connection(self):
        """Get the underlying database connection"""
        return self.connection