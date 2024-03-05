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
        print(f'Creating database schema: {args.name}')
        
        
class Stage(Command):
    def __init__(self, subp) -> None:
        stage_subp = subp.add_parser("stage")
        stage_subp.add_argument("schema", type=str)
        
    def run(self, args) -> None:
        print(f'Staging changes on database: {args.schema}')