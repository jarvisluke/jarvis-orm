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
        self.unqiue = unique
        self.primary_key = primary_key
        self.value = self.default

    def __repr__(self):
        return str(self.value)
    
    def set_value(self, value):
        # Checks the type of the value is supported by the Field subclass' affinity
        if (t := type(value)) not in self.affinity.value:
            raise TypeError(f"Unsupported type '{t}' for affinity {self.affinity}")
        self.value = value
    
    
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
        fields = self._get_fields()
        
        # Checks if any values in kwargs are not attributes of type Field
        if (diff := len(fields) - len(kwargs.keys())):
            mismatch = fields - kwargs.keys()
            if diff > 1:
                raise ValueError(f"Unexpected keyword arguments {mismatch}")
            else:
                raise TypeError(f"__init__() missing {diff} required keyword arguments: {mismatch}")
        
        # Initializes value attribute of each attribute of type Field 
        for k, v in kwargs.items():
            if k in fields:
                attr = getattr(self, k)
                attr.set_value(v)
            
    
    def _get_fields(self) -> List[Field]:
        attrs = self.__class__.__dict__
        fields = []
        
        for k, v in attrs.items():
            if isinstance(v, Field):
                fields.append(k)
                
        return fields