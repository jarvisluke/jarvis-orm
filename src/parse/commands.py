import os
from sys import stdout
from colorama import Fore, Back, Style

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
        stdout.write(Style.BRIGHT)
        match val:
            case 0:
                print(f"{Fore.GREEN}Created {name} at {path}")
            case 1:
                print(f"{Fore.RED}[!] {name} already exists at {path}")
            case -1:
                print(f"{Fore.RED}[!] Error creating {name} at {path}")
        stdout.write(Style.RESET_ALL)
                
                
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
        stdout.write(Style.BRIGHT)
        while True:
            confirm = input(f"Are you sure you want to drop {name}? [y/n]\n")
            if confirm:        
                if confirm.lower()[0] == "y":
                    break
                elif confirm.lower()[0] == "n":
                    print(f"{Fore.RED}[!] Aborted drop")
                    return
        path = os.getcwd() + "\\"
        val = utilities.drop_schema(path, name)
        match val:
            case 0:
                print(f"{Fore.GREEN}Dropped {name} at {path}")
            case 1:
                print(f"{Fore.RED}[!] {name} does not exists at {path}")
            case -1:
                print(f"{Fore.RED}[!] Error dropping {name} at {path}")
        stdout.write(Style.RESET_ALL)
        
        
class Stage(Command):
    def __init__(self, subp) -> None:
        stage_subp = subp.add_parser("stage")
        stage_subp.add_argument("schema", type=str)
        
    def run(self, args) -> None:
        print(f'Staging changes on database: {args.schema}')