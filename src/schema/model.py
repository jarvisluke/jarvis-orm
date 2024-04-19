from enum import Enum
from types import NoneType
from typing import List


class Affinity(Enum):
    NONE = (NoneType,)
    INTEGER = (int,)
    TEXT = (str,)
    BLOB = (bytes, bytearray, memoryview)
    REAL = (float,)


class Field:
    affinity = Affinity.NONE
    
    def __init__(self, not_null=True, default=None, unique=False, primary_key=False) -> None:
        self.not_null = not_null
        self.default = default
        self.unique = unique
        self.primary_key = primary_key
        self.value = self.default
        
    def __repr__(self):
        return str(self.value)
    
    def set_value(self, value) -> None:
        # Checks the type of the value is supported by the class's affinity
        if (t := type(value)) not in self.affinity.value:
            raise TypeError(f"Unsupported type '{t}' for affinity {self.affinity}")
        self.value = value
        
    def get_options_string(self) -> str:
        s = ""
        if self.not_null:
            s += "NOT NULL "
        if self.unique:
            s += "UNIQUE "
        if self.primary_key:
            s += "PRIMARY KEY "
        return s[:-1]
    
    
class IntegerField(Field):
    affinity = Affinity.INTEGER

    
class TextField(Field):
    affinity = Affinity.TEXT

    
class BlobField(Field):
    affinity = Affinity.BLOB
    
    
class RealField(Field):
    affinity = Affinity.REAL
            
        
class Table:
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
        
        # Initializes value attribute of each attribute of type Field 
        for k, v in kwargs.items():
            if k in fields:
                attr = getattr(self, k)
                attr.set_value(v)
    
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
    
    def get_primary_key(self) -> str:
        for field in self.get_fields():
            if getattr(self, field).primary_key:
                return field
            
    @classmethod
    def get_primary_key_cls(cls) -> str:
        for field in cls.get_fields_cls():
            obj = getattr(cls, field)
            if isinstance(obj, Field) and obj.primary_key:
                return field
    
    @classmethod
    def get_create_query(cls) -> tuple[str | tuple]:
        name = cls.__name__.lower()
        
        # Iterates through each field, building a string containing its name, affinity, and options
        fields = []
        for field in cls.get_fields_cls():
            options = getattr(cls, field).get_options_string()
            s = ""
            for o in options:
                s += o
            s = f"{field} {getattr(cls, field).affinity.name} {s}"
            fields.append(s)
            
        # Removes the last element's comma
        fields[-1] = fields[-1][:-1]
        
        return f"CREATE TABLE {name} ({', '.join(fields)});"
        
    def get_insert_query(self) -> tuple[str | tuple]:
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
    
    def get_update_query(self) -> tuple[str | tuple]:
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
    
    def save(self, t):
        if t.exists(self):
            t.update(self)
        else:
            t.insert(self)