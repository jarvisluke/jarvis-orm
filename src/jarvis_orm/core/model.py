from enum import Enum
from types import NoneType
from typing import Dict, List


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
    
    def set_value(self, value) -> None:
        if self.foreign_key:
            if t:= self.foreign_key == value.__class__:
                value = getattr(value, value.get_primary_key()).value
        # Checks the type of the value is supported by the class's affinity
        if (t := type(value)) not in self.affinity.value:
            raise TypeError(f"Unsupported type '{t}' for affinity {self.affinity}")
        self.value = value
        
    def get_options_string(self) -> str:
        ls: List[str] = []
        if self.not_null:
            ls.append("NOT NULL")
        if self.unique:
            ls.append("UNIQUE")       
        if self.primary_key:
            ls.append("PRIMARY KEY")
        return " ".join(ls)
    
    def get_foreign_key_options_string(self) -> str | None:
        if self.foreign_key:
            return f"ON UPDATE {self.on_update} ON DELETE {self.on_delete}"
        else:
            return None
    
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
        keys = list(kwargs.keys())
        
        # Checks if any values in kwargs are not attributes of type Field
        if (diff := len(fields) - len(keys)):
            mismatch = list(set(fields) - set(keys)) + list(set(keys) - set(fields))
            if diff > 0:
                # Checks if all fields not filled have not_null == True
                if not all([not getattr(self, f).not_null for f in mismatch]):
                    raise TypeError(f"__init__() missing {diff} required not null keyword arguments: {mismatch}")
            elif diff < 0:
                raise ValueError(f"Unexpected keyword arguments {mismatch}")
        
        """ # Initializes value attribute of each attribute of type Field 
        for k, v in kwargs.items():
            if k in fields:
                attr = getattr(self, k)
                attr.set_value(v) """
                
        print("field args:", self.get_field_attrs())
                
        # Sets value of each field to its corresponding keyword argument
        field_attrs = self.get_field_attrs()
        for k, v in field_attrs.items():
            if k in kwargs:
                
                
            
                
    @classmethod
    def get_name(cls) -> str:
        return cls.__name__.lower()
    
    def get_field_attrs(self) -> dict[str, Field]:
        attrs = self.__class__.__dict__.items()
        field_attrs = {}
        
        for k, v in attrs:
            if isinstance(v, Field):
                field_attrs[k] = v
        
        return field_attrs
    
    def get_fields(self) -> List[Field]:
        attrs = self.__class__.__dict__
        fields = []
        
        # Returns names of all attributes of type Field
        for k, v in attrs.items():
            if isinstance(v, Field):
                fields.append(k)
                
        return fields
    
    @classmethod
    def get_fields_cls(cls) -> List[Field]:
        attrs = cls.__dict__
        fields = []
        
        for k, v in attrs.items():
            if isinstance(v, Field):
                fields.append(k)
                
        return fields
    
    def get_primary_key(self):
        for field in self.get_fields():
            if getattr(self, field).primary_key:
                return field
            
    @classmethod
    def get_primary_key_cls(cls):
        for field in cls.get_fields_cls():
            obj = getattr(cls, field)
            if isinstance(obj, Field) and obj.primary_key:
                return field
    
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