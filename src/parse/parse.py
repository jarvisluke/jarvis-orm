from argparse import ArgumentParser
from typing import Dict

from .commands import Command, Create, Stage


class CommandParser:
    """Parses command line and executes appropriate commands"""
    def __init__(self, cmds: Dict[str, Command]) -> None:
        self.parser = ArgumentParser()
        self.subp = self.parser.add_subparsers(dest="cmd")
        self.cmds: Dict[str, Command]= {}
        # Initializes commands
        for name, cls in cmds.items():
            self.cmds[name] = cls(self.subp)
        
    def parse_args(self):
        args = self.parser.parse_args() 
        cmd: Command = self.cmds[args.cmd]
        cmd.run(args)
        

def parse():
    parser = CommandParser({
        "create": Create,
        "stage": Stage,
    })
    parser.parse_args()