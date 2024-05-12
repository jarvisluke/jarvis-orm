from model import Field, Table


# Returns name of Table in database
def get_name(table: type[Table]) -> str:
    return table.__name__.lower()

# Returns a list of names of Field object attributes of a class
def get_fields(table: type[Table]) -> dict[str, Field]:
    return {k: v for k, v in table.__dict__.items() if isinstance(v, Field)}

# Returns the name of the primary key Field object
def get_primary_key(table: type[Table]) -> dict[str, Field]:
    return {k: v for k, v in table.__dict__.items() if isinstance(v, Field) and v.primary_key}