import sys
sys.path.append("..")

import os

from engine import utilities


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
        val = utilities.create_schema(path, name)
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
        
    def run(self, args) -> None:
        name = args.name
        if not name.endswith(".db"):
            name += ".db"
        path = os.getcwd() + "\\"
        val = utilities.drop_schema(path, name)
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
        print(f'Staging changes on database: {args.schema}')