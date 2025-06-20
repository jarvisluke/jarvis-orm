from enum import Enum
from types import NoneType

from .util import get_primary_key


class Affinity(Enum):
    NONE = (NoneType,)
    INTEGER = (int,)
    TEXT = (str,)
    BLOB = (bytes, bytearray, memoryview)
    REAL = (float,)
    

class Constraint(Enum):
    RESTRICT = "RESTRICT"
    NO_ACTION = "NO ACTION"
    CASCADE = "CASCADE"
    SET_NULL = "SET NULL"


class Field:
    affinity: Affinity = Affinity.NONE
    
    def __init__(
        self, not_null=True, default=None, unique=False, primary_key=False, # Regular options
        foreign_key=None, on_update=Constraint.RESTRICT, on_delete=Constraint.RESTRICT # Foreign key options
        ) -> None:
        self.not_null = not_null
        self.unique = unique
        self.primary_key = primary_key
        self.foreign_key = foreign_key
        self.on_update: Constraint
        self.on_delete: Constraint
        
        # Set foreign key options
        if self.foreign_key:
            self.on_update = on_update
            self.on_delete = on_delete
        else:
            self.on_update = None
            self.on_delete = None
        
        self.value = default
        
    def __str__(self):
        return str(self.value)
    
    def get_options(self) -> list[str]:
        options = []
        if self.not_null:
            options.append("not_null")
        if self.unique:
            options.append("unique")
        if self.primary_key:
            options.append("primary_key")
        if self.foreign_key:
            options.append("foreign_key")
            if self.on_update:
                options.append("on_update")
            if self.on_delete:
                options.append("on_delete")
        return options
    
    def get_options_string(self) -> str:
        """Return SQL options string for field definition"""
        options = []
        if self.not_null:
            options.append("NOT NULL")
        if self.unique:
            options.append("UNIQUE")
        if self.primary_key:
            options.append("PRIMARY KEY")
        return " ".join(options)
    
    def set(self, value) -> None:
        # TODO: fix it
            
        if isinstance(value, Table):
            value = next(iter(value.get_primary_key()))
                
        # Checks the type of the value is supported by the class's affinity
        elif (t := value.__class__) not in self.affinity.value:
            raise TypeError(f"Unsupported type '{t.__name__}' for {self.affinity}")
        self.value = value
    
    
class IntegerField(Field):
    affinity = Affinity.INTEGER

    
class TextField(Field):
    affinity = Affinity.TEXT

"""
NOT YET IMPLEMENTED

class BlobField(Field):
    affinity = Affinity.BLOB
 """    
    
class RealField(Field):
    affinity = Affinity.REAL
    

class TableMeta(type):
    def __new__(cls, name, bases, dct):
        new_cls = super().__new__(cls, name, bases, dct)
        
        # Add set_attr method for each attribute
        for attr in dct:
            if not attr.startswith('__') and not callable(dct[attr]):
                setattr(new_cls, f'set_{attr}', cls.make_set_attr(attr))
        
        return new_cls
    
    @staticmethod
    def make_set_attr(attr):
        def set_attr(self, value):
            getattr(self, attr).set_value(value)
        return set_attr
    
        
class Table(metaclass=TableMeta):
    def __init__(self, **kwargs) -> None:
        fields = self.get_fields()
        
        # Checks all 'not null' fields are in kwargs
        for k, v in fields.items():
            # If any 'not null' fields are not in the object instantiation
            if k not in kwargs.keys() and v.not_null:
                raise TypeError(f"{self.__class__.__name__}.__init__() missing keyword argument: '{k}'")
            
        # Initialize fields in kwargs
        for k, v in kwargs.items():
            # If any keyword argument is not in fields
            if k not in fields.keys():
                raise TypeError(f"{self.__class__.__name__}.__init__() received unexpected keyword argument: '{k}'")
            fields[k].set(v)
    
    # Returns dict of name and object of all Field attributes
    def get_fields(self) -> dict[str, Field]:
        return {k: v for k, v in self.__class__.__dict__.items() if isinstance(v, Field)}
    
    # Returns dict of the name and object of the primary key Field
    def get_primary_key(self) -> dict[str, Field]:
        return {k: v for k, v in self.__class__.__dict__.items() if isinstance(v, Field) and v.primary_key}
    
    @classmethod
    def get_fields_cls(cls) -> list[str]:
        """Return list of field names for the class"""
        return [k for k, v in cls.__dict__.items() if isinstance(v, Field)]
    
    @classmethod
    def get_primary_key_cls(cls) -> str:
        """Return the name of the primary key field"""
        for k, v in cls.__dict__.items():
            if isinstance(v, Field) and v.primary_key:
                return k
        return None
    
    @classmethod
    def get_create_query(cls) -> str:
        """
        Generate CREATE TABLE query using proper SQL construction.
        Note: This method is kept for backward compatibility but should use
        parameterized queries where possible in actual database operations.
        """
        name = cls.__name__.lower()
        
        # Validate table name to prevent injection
        if not name.replace('_', '').isalnum():
            raise ValueError("Invalid table name")
        
        # Iterates through each field, building a string containing its name, affinity, and options
        fields = []
        for field in cls.get_fields_cls():
            attr: Field = getattr(cls, field)
            
            # Validate field name
            if not field.replace('_', '').isalnum():
                raise ValueError(f"Invalid field name: {field}")
            
            # Formats options
            options = attr.get_options_string()
            s = f"{field} {attr.affinity.name}"
            if options:
                s += " " + options
            fields.append(s)
            
            if attr.foreign_key:
                fk_table_name = attr.foreign_key.__name__.lower()
                fk_pk_field = attr.foreign_key.get_primary_key_cls()
                
                # Validate foreign key table and field names
                if not fk_table_name.replace('_', '').isalnum():
                    raise ValueError(f"Invalid foreign key table name: {fk_table_name}")
                if not fk_pk_field.replace('_', '').isalnum():
                    raise ValueError(f"Invalid foreign key field name: {fk_pk_field}")
                
                fields.append(f"FOREIGN KEY ({field}) REFERENCES {fk_table_name}({fk_pk_field})")
        
        return f"CREATE TABLE {name} ({', '.join(fields)});"
        
    def get_insert_params(self) -> tuple[str, tuple]:
        """
        Generate INSERT query with parameterized values.
        Returns: (query_string, parameter_tuple)
        """
        name = self.__class__.__name__.lower()
        
        # Validate table name
        if not name.replace('_', '').isalnum():
            raise ValueError("Invalid table name")
        
        # Get fields that have values
        fields_with_values = []
        values = []
        
        for field_name, field_obj in self.get_fields().items():
            if field_obj.value is not None:
                fields_with_values.append(field_name)
                values.append(field_obj.value)
        
        # Build parameterized query
        placeholders = ', '.join(['?'] * len(values))
        query = f"INSERT INTO {name} ({', '.join(fields_with_values)}) VALUES ({placeholders})"
        
        return query, tuple(values)
    
    def get_update_params(self) -> tuple[str, tuple]:
        """
        Generate UPDATE query with parameterized values.
        Returns: (query_string, parameter_tuple)
        """
        name = self.__class__.__name__.lower()
        
        # Validate table name
        if not name.replace('_', '').isalnum():
            raise ValueError("Invalid table name")
        
        # Get fields that have values
        fields_with_values = []
        values = []
        
        for field_name, field_obj in self.get_fields().items():
            if field_obj.value is not None:
                fields_with_values.append(field_name)
                values.append(field_obj.value)
        
        # Get primary key
        pk_dict = self.get_primary_key()
        pk_name = next(iter(pk_dict.keys()))
        pk_value = next(iter(pk_dict.values()))
        
        # Build SET clauses with placeholders
        set_clauses = ', '.join([f"{field} = ?" for field in fields_with_values])
        
        # Build complete query
        query = f"UPDATE {name} SET {set_clauses} WHERE {pk_name} = ?"
        
        # Add primary key value to parameters
        values.append(pk_value)
        
        return query, tuple(values)

    # Legacy methods - kept for backward compatibility but not recommended for use
    def get_insert_string(self) -> str:
        """
        DEPRECATED: This method uses string formatting and may be vulnerable to injection.
        Use get_insert_params() instead for secure parameterized queries.
        """
        name = self.__class__.__name__.lower()
        
        # Lists names of all fields whose value is not None
        fields = [f for f in self.get_fields()]
        fields = [f for f in fields if getattr(self, f).value]
        
        # Lists values of all fields
        values = [str(getattr(self, f).value) for f in fields]
        
        # Adds quotation marks around values of fields with text affinities
        for i in range(len(values)):
            if getattr(self, fields[i]).affinity == Affinity.TEXT:
                values[i] = '"' + values[i] + '"'
                
        return f"INSERT INTO {name} ({', '.join(fields)}) VALUES ({', '.join(values)});"
    
    def get_update_string(self) -> str:
        """
        DEPRECATED: This method uses string formatting and may be vulnerable to injection.
        Use get_update_params() instead for secure parameterized queries.
        """
        name = self.__class__.__name__.lower()
        
        # Lists names of all fields whose value is not None
        fields = [f for f in self.get_fields()]
        fields = [f for f in fields if getattr(self, f).value]
        
        # Lists values of all fields
        values = [str(getattr(self, f).value) for f in fields]
        
        # Adds quotation marks around values of fields with text affinities
        for i in range(len(values)):
            if getattr(self, fields[i]).affinity == Affinity.TEXT:
                values[i] = '"' + values[i] + '"'
                
        # Builds field = value statements
        set_fields = []
        for t in zip(fields, values):
            set_fields.append(t[0] + " = " + t[1])
            
        # Build pk = pk statement
        pk = self.get_primary_key()
        pk_name = next(iter(pk.keys()))
        pk_value = getattr(self, pk_name).value
                
        return f"UPDATE {name} SET {', '.join(set_fields)} WHERE {pk_name} = {pk_value};"
