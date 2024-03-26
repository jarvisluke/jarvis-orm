def build(model, query):
    return model(query)


class Field:
    def __init__(self, NOT_NULL=False, DEFAULT=None, UNIQUE=False, PRIMARY_KEY=False) -> None:
        self.NOT_NULL = NOT_NULL
        self.DEFAULT = DEFAULT
        self.UNIQUE = UNIQUE
        self.PRIMARY_KEY = PRIMARY_KEY
        
        self.value = self.DEFAULT


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
