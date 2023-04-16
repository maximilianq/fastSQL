from psycopg2 import connect
from psycopg2.extensions import connection as Connection
from uuid import UUID as uuid, uuid4
from re import compile
from typing import Pattern, List, Tuple, Type, Dict

class FastSQL:

    def __init__(self, host: str, port: int, databse: str, username: str, password: str) -> None:
        self.host: str = host
        self.port: int = port
        self.database: str = databse
        self.username: str = username
        self.password: str = password
        self.connection: Connection = None
        self.queries: List[Query] = []
    
    def execute(self, query: str, *args):

        with self.connection.cursor() as cursor:
            
            if not args:
                cursor.execute(query)
            else:
                cursor.execute(query, *args)

            if cursor.description:
                return cursor.fetchall()
            else:
                return None

    def include_query(self, query: 'Query'):
        query.include(self)
        self.queries.append(query)

    def include_router(self, router: 'SQLRouter'):
        router.include(self)
        for query in router.queries:
            self.queries.append(query)

    def start(self):
        self.connection = connect(dbname=self.database, user=self.username, password=self.password, host=self.host, port=self.port)
        self.connection.autocommit = True

        for query in self.queries:
            query.prepare()
    
    def stop(self):
        self.connection.close()

class SQLRouter:

    def __init__(self) -> None:
        self.client: FastSQL = None
        self.queries: List[Query] = []

    def include(self, client: FastSQL):
        self.client = client
    
    def include_query(self, query: 'Query'):
        query.include(self)
        self.queries.append(query)

    def execute(self, query: str, *args):
        return self.client.execute(query, args)

class Query:

    pattern: Pattern = compile(r'\%\(([a-z]+)\)([a-z])')
    types: Dict[str, Type] = {'s': str}

    def __init__(self, query: str) -> None:

        self.client: FastSQL | SQLRouter = None

        self.uuid: uuid = uuid4()
        
        self.prepare_query: str = f'PREPARE "{str(self.uuid)}" AS {query};'
        
        self.parameters: List[Tuple[str, Type]] = [(name, self.types.get(type, None)) for name, type in set(self.pattern.findall(query))]
        for index, (name, type) in enumerate(self.parameters):
            self.prepare_query =  self.prepare_query.replace(f"%({name})s", f'${index + 1}')

        self.execute_query: str = f'EXECUTE "{str(self.uuid)}"' + (f'({", ".join(["%s"] * len(self.parameters))})' if len(self.parameters) > 0 else '') + ';'

    def include(self, client: FastSQL | SQLRouter):
        self.client = client

    def prepare(self):
        print(self.prepare_query)
        self.client.execute(self.prepare_query)

    def execute(self, **kwargs):
        args: List[object] = [kwargs.get(name, None) for (name, type) in self.parameters]
        return self.client.execute(self.execute_query, *args)
    
class SimpleQuery(Query):

    def __init__(self, query: str) -> None:
        super().__init__(query.rstrip(';'))

class FileQuery(Query):

    def __init__(self, path: str) -> None:

        query: str = None
        with open(path, 'r') as file:
            query = file.read()

        super().__init__(query.rstrip(';'))