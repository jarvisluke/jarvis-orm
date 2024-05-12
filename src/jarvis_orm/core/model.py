from enum import Enum
from types import NoneType
from typing import Dict, List, Type


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
        
    def __repr__(self):
        return str(self.value)
    
    def __str__(self):
        return str(self.value)
    
    def set(self, value) -> None:
        if self.foreign_key:
            if t:= self.foreign_key == value.__class__:
                value = getattr(value, value.get_primary_key()).value
        # Checks the type of the value is supported by the class's affinity
        if (t := type(value)) not in self.affinity.value:
            raise TypeError(f"Unsupported type '{t.__name__}' for {self.affinity}")
        self.value = value
    
    
class IntegerField(Field):
    affinity = Affinity.INTEGER

    
class TextField(Field):
    affinity = Affinity.TEXT

    
class BlobField(Field):
    affinity = Affinity.BLOB
    
    
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
    def get_fields(self) -> Dict[str, Field]:
        return {k: v for k, v in self.__class__.__dict__.items() if isinstance(v, Field)}
    
    # Returns dict of the name and object of the primary key Field
    def get_primary_key(self) -> Dict[str, Field]:
        return {k: v for k, v in self.__class__.__dict__.items() if isinstance(v, Field) and v.primary_key}
    
    @classmethod
    def get_create_query(cls) -> str:
        
        name = cls.__name__.lower()
        
        # Iterates through each field, building a string containing its name, affinity, and options
        fields = []
        foreign_keys = []
        for field in cls.get_fields_cls():
            attr: Field = getattr(cls, field)
            
            # Formats options
            options = attr.get_options_string()
            s = f"{field} {attr.affinity.name}"
            if options:
                s += " " + options
            fields.append(s)
            
            if attr.foreign_key:
                fields.append(f"FOREIGN KEY ({field}) REFERENCES {attr.foreign_key.__name__.lower()}({attr.foreign_key.get_primary_key_cls()})")
        
        return f"CREATE TABLE {name} ({', '.join(fields)});"
        
    def get_insert_string(self) -> str:
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
    
    def get_update_string(self) -> tuple[str | tuple]:
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
        pk = pk + " = " + getattr(self, pk).value
                
        return f"UPDATE {name} SET {', '.join(set_fields)} WHERE {pk};"