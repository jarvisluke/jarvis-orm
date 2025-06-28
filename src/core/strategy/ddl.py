from typing import List, Dict, Any, Optional, Type, Callable
from enum import Enum
from dataclasses import dataclass
import logging
from ..model import Table
from ..util import get_name
from .dependency import DependencyGraph


class OperationType(Enum):
    """Types of DDL operations"""
    CREATE_TABLE = "CREATE_TABLE"
    DROP_TABLE = "DROP_TABLE"
    ALTER_TABLE = "ALTER_TABLE"
    CREATE_INDEX = "CREATE_INDEX"
    DROP_INDEX = "DROP_INDEX"


@dataclass
class DDLOperation:
    """Represents a single DDL operation"""
    operation_type: OperationType
    table: Type[Table]
    details: Optional[Dict[str, Any]] = None
    
    def __str__(self):
        return f"{self.operation_type.value}: {get_name(self.table)}"


class DDLStrategy:
    """
    Manages DDL operations with dependency awareness and transaction support.
    """
    
    def __init__(self, adapter, logger: Optional[logging.Logger] = None):
        self.adapter = adapter
        self.logger = logger
        self.dependency_graph = DependencyGraph()
        self.operations: List[DDLOperation] = []
        self.executed_operations: List[DDLOperation] = []
        
    def register_tables(self, tables: List[Type[Table]]) -> None:
        """Register tables with the dependency graph"""
        self.dependency_graph.add_tables(tables)
        
        if self.logger:
            self.logger.info(f"Registered {len(tables)} tables with DDL strategy")
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(self.dependency_graph.visualize())
                
    def plan_create_all(self) -> List[DDLOperation]:
        """
        Plan creation of all registered tables in dependency order.
        Returns list of operations without executing them.
        """
        operations = []
        creation_order = self.dependency_graph.get_creation_order()
        
        for table in creation_order:
            op = DDLOperation(OperationType.CREATE_TABLE, table)
            operations.append(op)
            
        if self.logger:
            self.logger.info(f"Planned {len(operations)} CREATE TABLE operations")
            if self.logger.isEnabledFor(logging.DEBUG):
                for i, op in enumerate(operations, 1):
                    self.logger.debug(f"  {i}. {op}")
                    
        return operations
        
    def plan_drop_all(self) -> List[DDLOperation]:
        """
        Plan deletion of all registered tables in reverse dependency order.
        Returns list of operations without executing them.
        """
        operations = []
        deletion_order = self.dependency_graph.get_deletion_order()
        
        for table in deletion_order:
            op = DDLOperation(OperationType.DROP_TABLE, table)
            operations.append(op)
            
        if self.logger:
            self.logger.info(f"Planned {len(operations)} DROP TABLE operations")
            if self.logger.isEnabledFor(logging.DEBUG):
                for i, op in enumerate(operations, 1):
                    self.logger.debug(f"  {i}. {op}")
                    
        return operations
        
    def execute_operation(self, operation: DDLOperation) -> None:
        """Execute a single DDL operation"""
        if self.logger:
            self.logger.info(f"Executing: {operation}")
            
        try:
            if operation.operation_type == OperationType.CREATE_TABLE:
                self.adapter.create_table(operation.table)
            elif operation.operation_type == OperationType.DROP_TABLE:
                self.adapter.drop_table(operation.table)
            else:
                raise NotImplementedError(f"Operation {operation.operation_type} not implemented")
                
            self.executed_operations.append(operation)
            
            if self.logger:
                self.logger.info(f"Successfully executed: {operation}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to execute {operation}: {e}")
            raise
            
    def execute_operations(self, operations: List[DDLOperation], 
                         stop_on_error: bool = True,
                         callback: Optional[Callable[[DDLOperation, bool, Optional[Exception]], None]] = None) -> Dict[str, Any]:
        """
        Execute a list of DDL operations.
        
        Args:
            operations: List of operations to execute
            stop_on_error: If True, stop on first error. If False, continue with remaining operations.
            callback: Optional callback function called after each operation (op, success, exception)
            
        Returns:
            Dictionary with execution results
        """
        results = {
            'total': len(operations),
            'successful': 0,
            'failed': 0,
            'errors': []
        }
        
        for operation in operations:
            try:
                self.execute_operation(operation)
                results['successful'] += 1
                
                if callback:
                    callback(operation, True, None)
                    
            except Exception as e:
                results['failed'] += 1
                results['errors'].append({
                    'operation': str(operation),
                    'error': str(e),
                    'type': type(e).__name__
                })
                
                if callback:
                    callback(operation, False, e)
                    
                if stop_on_error:
                    break
                    
        if self.logger:
            self.logger.info(f"Execution complete: {results['successful']}/{results['total']} successful")
            
        return results
        
    def create_all(self, stop_on_error: bool = True) -> Dict[str, Any]:
        """Create all registered tables in dependency order"""
        operations = self.plan_create_all()
        return self.execute_operations(operations, stop_on_error)
        
    def drop_all(self, stop_on_error: bool = True) -> Dict[str, Any]:
        """Drop all registered tables in reverse dependency order"""
        operations = self.plan_drop_all()
        return self.execute_operations(operations, stop_on_error)
        
    def get_parallel_groups(self, operations: List[DDLOperation]) -> List[List[DDLOperation]]:
        """
        Group operations that can be executed in parallel.
        Operations at the same dependency level can be executed simultaneously.
        """
        levels = self.dependency_graph.get_dependency_levels()
        groups = []
        
        for level in sorted(levels.keys()):
            group = []
            level_tables = {get_name(t) for t in levels[level]}
            
            for op in operations:
                if get_name(op.table) in level_tables:
                    group.append(op)
                    
            if group:
                groups.append(group)
                
        return groups
        
    def validate_operations(self, operations: List[DDLOperation]) -> List[str]:
        """
        Validate a list of operations for potential issues.
        Returns list of warning/error messages.
        """
        issues = []
        
        # Check for duplicate operations
        seen = set()
        for op in operations:
            key = (op.operation_type, get_name(op.table))
            if key in seen:
                issues.append(f"Duplicate operation: {op}")
            seen.add(key)
            
        # Check for operations on unregistered tables
        registered = set(self.dependency_graph.tables.keys())
        for op in operations:
            if get_name(op.table) not in registered:
                issues.append(f"Operation on unregistered table: {op}")
                
        # Check for DROP operations on tables with dependents
        for op in operations:
            if op.operation_type == OperationType.DROP_TABLE:
                dependents = self.dependency_graph.get_dependents(op.table)
                if dependents:
                    dep_names = [get_name(t) for t in dependents]
                    issues.append(f"DROP {get_name(op.table)} has dependents: {', '.join(dep_names)}")
                    
        return issues