from typing import Dict, List, Set, Type, Optional, Tuple
from collections import defaultdict, deque
from ..model import Table, Field
from ..util import get_fields, get_name


class CircularDependencyError(Exception):
    """Raised when circular dependencies are detected in the table graph"""
    def __init__(self, cycle: List[str]):
        self.cycle = cycle
        super().__init__(f"Circular dependency detected: {' -> '.join(cycle)}")


class DependencyGraph:
    """
    Manages table dependencies and provides ordering for DDL operations.
    """
    
    def __init__(self):
        self.tables: Dict[str, Type[Table]] = {}
        self.dependencies: Dict[str, Set[str]] = defaultdict(set)  # table -> set of tables it depends on
        self.dependents: Dict[str, Set[str]] = defaultdict(set)    # table -> set of tables that depend on it
        
    def add_table(self, table: Type[Table]) -> None:
        """Add a table to the dependency graph"""
        table_name = get_name(table)
        self.tables[table_name] = table
        
        # Analyze foreign key dependencies
        fields = get_fields(table)
        for field_name, field in fields.items():
            if field.foreign_key:
                fk_table_name = get_name(field.foreign_key)
                self.dependencies[table_name].add(fk_table_name)
                self.dependents[fk_table_name].add(table_name)
                
    def add_tables(self, tables: List[Type[Table]]) -> None:
        """Add multiple tables to the dependency graph"""
        for table in tables:
            self.add_table(table)
            
    def _detect_cycle(self) -> Optional[List[str]]:
        """
        Detect if there's a cycle in the dependency graph.
        Returns the cycle path if found, None otherwise.
        """
        visited = set()
        rec_stack = set()
        path = []
        
        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in self.dependencies.get(node, set()):
                if neighbor not in visited:
                    cycle = dfs(neighbor)
                    if cycle:
                        return cycle
                elif neighbor in rec_stack:
                    # Found a cycle
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:] + [neighbor]
            
            path.pop()
            rec_stack.remove(node)
            return None
        
        for table in self.tables:
            if table not in visited:
                cycle = dfs(table)
                if cycle:
                    return cycle
        return None
        
    def _topological_sort(self) -> List[str]:
        """
        Perform topological sort on the dependency graph.
        Returns ordered list of table names.
        """
        # Check for cycles first
        cycle = self._detect_cycle()
        if cycle:
            raise CircularDependencyError(cycle)
        
        # Kahn's algorithm for topological sort
        in_degree = defaultdict(int)
        for table in self.tables:
            for dep in self.dependencies.get(table, set()):
                in_degree[dep] += 1
        
        # Find all nodes with no incoming edges
        queue = deque([table for table in self.tables if in_degree[table] == 0])
        result = []
        
        while queue:
            node = queue.popleft()
            result.append(node)
            
            # Remove edge from graph
            for dependent in self.dependents.get(node, set()):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        if len(result) != len(self.tables):
            # This shouldn't happen if cycle detection works correctly
            raise CircularDependencyError(["Unknown cycle in graph"])
        
        return result
        
    def get_creation_order(self) -> List[Type[Table]]:
        """
        Get the order in which tables should be created.
        Tables with no dependencies come first.
        """
        ordered_names = self._topological_sort()
        return [self.tables[name] for name in ordered_names]
        
    def get_deletion_order(self) -> List[Type[Table]]:
        """
        Get the order in which tables should be deleted.
        This is the reverse of creation order.
        """
        return list(reversed(self.get_creation_order()))
        
    def get_dependencies(self, table: Type[Table]) -> Set[Type[Table]]:
        """Get all tables that the given table depends on"""
        table_name = get_name(table)
        dep_names = self.dependencies.get(table_name, set())
        return {self.tables[name] for name in dep_names}
        
    def get_dependents(self, table: Type[Table]) -> Set[Type[Table]]:
        """Get all tables that depend on the given table"""
        table_name = get_name(table)
        dep_names = self.dependents.get(table_name, set())
        return {self.tables[name] for name in dep_names}
        
    def get_dependency_levels(self) -> Dict[int, List[Type[Table]]]:
        """
        Group tables by dependency level.
        Level 0 has no dependencies, level 1 depends only on level 0, etc.
        Tables at the same level can be processed in parallel.
        """
        levels = defaultdict(list)
        table_levels = {}
        
        def get_level(table_name: str) -> int:
            if table_name in table_levels:
                return table_levels[table_name]
            
            deps = self.dependencies.get(table_name, set())
            if not deps:
                level = 0
            else:
                level = max(get_level(dep) for dep in deps) + 1
            
            table_levels[table_name] = level
            return level
        
        for table_name, table in self.tables.items():
            level = get_level(table_name)
            levels[level].append(table)
        
        return dict(levels)
        
    def visualize(self) -> str:
        """Generate a text representation of the dependency graph"""
        lines = ["Dependency Graph:"]
        lines.append("=" * 50)
        
        for table_name in sorted(self.tables.keys()):
            deps = self.dependencies.get(table_name, set())
            if deps:
                lines.append(f"{table_name} -> {', '.join(sorted(deps))}")
            else:
                lines.append(f"{table_name} (no dependencies)")
        
        lines.append("\nDependency Levels:")
        lines.append("-" * 50)
        levels = self.get_dependency_levels()
        for level in sorted(levels.keys()):
            tables = [get_name(t) for t in levels[level]]
            lines.append(f"Level {level}: {', '.join(sorted(tables))}")
        
        return "\n".join(lines)