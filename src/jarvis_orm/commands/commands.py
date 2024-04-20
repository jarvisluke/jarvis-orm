import os
import sqlite3

from ..core.utilities import create_schema, drop_schema


class Command:
    """Base type"""
    def __init__(self) -> None:
        pass
    
    def setup():
        pass

    def run():
        pass
    
    
class Create(Command):
    def __init__(self, subp) -> None:
        create_subp = subp.add_parser("create")
        create_subp.add_argument("name", type=str)
        
    def run(self, args) -> None:
        name = args.name
        if not name.endswith(".db"):
            name += ".db"
        path = os.getcwd() + "\\"
        val = create_schema(path, name)
        match val:
            case 0:
                print(f"Created {name} at {path}")
            case 1:
                print(f"[!] {name} already exists at {path}")
            case -1:
                print(f"[!] Error creating {name} at {path}")
                
                
class Drop(Command):
    def __init__(self, subp) -> None:
        create_subp = subp.add_parser("drop")
        create_subp.add_argument("name", type=str)
        # TODO: add optional -y parameter
        
    def run(self, args) -> None:
        name = args.name
        if not name.endswith(".db"):
            name += ".db"
        # Checks user input
        while True:
            confirm = input(f"Are you sure you want to drop {name}? [y/n]\n")
            if confirm:        
                if confirm.lower()[0] == "y":
                    break
                elif confirm.lower()[0] == "n":
                    print(f"[!] Aborted drop")
                    return
        path = os.getcwd() + "\\"
        val = drop_schema(path, name)
        match val:
            case 0:
                print(f"Dropped {name} at {path}")
            case 1:
                print(f"[!] {name} does not exists at {path}")
            case -1:
                print(f"[!] Error dropping {name} at {path}")
        
        
class Stage(Command):
    def __init__(self, subp) -> None:
        stage_subp = subp.add_parser("stage")
        stage_subp.add_argument("schema", type=str)
        
    def run(self, args) -> None:
        print('Staging changes on database: {args.schema}')
        
        
class Tables(Command):
    def __init__(self, subp) -> None:
        tables_subp = subp.add_parser("tables")
        tables_subp.add_argument("schema", type=str)
        
    def run(self, args) -> None:
        con = sqlite3.connect(args.schema)
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")

        tables = cur.fetchall()

        cur.close()
        con.close()

        print(tables)
        
        
# Add commands here
subprocessors =  {
    "create": Create,
    "drop": Drop,
    "stage": Stage,
    "tables": Tables,
}