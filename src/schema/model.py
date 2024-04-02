import enum


class Affinity(enum.Enum):
    NONE = "NONE"
    INTEGER = "INTEGER"
    TEXT = "TEXT"
    BLOB = "BLOB"
    REAL = "REAL"


class Field:
    affinity = Affinity.NONE
    
    def __init__(self, NOT_NULL=False, DEFAULT=None, UNIQUE=False, PRIMARY_KEY=False) -> None:
        self.NOT_NULL = NOT_NULL
        self.DEFAULT = DEFAULT
        self.UNIQUE = UNIQUE
        self.PRIMARY_KEY = PRIMARY_KEY
        self.value = self.DEFAULT
        
        
class IntegerField(Field):
    affinity = Affinity.INTEGER
        
    
class TextField(Field):
    affinity = Affinity.TEXT

    
class RealField(Field):
    affinity = Affinity.REAL
    
    
class BlobField(Field):
    affinity = Affinity.BLOB
    

class Model:
    def __init__(self, **kwargs) -> None:
        # Sets values of Field attributes to corresponding item in values
        fields = self._get_fields()
        for k, v in kwargs:
            fields[k].value = v
            
    
    def _get_fields(cls):
        attrs = cls.__dict__
        # Returns all attributes of the class which are an instance of Field
        return {k: v for (k, v) in attrs.items() if isinstance(v, Field)}
