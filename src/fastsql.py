from psycopg import connect
from psycopg.connection import Connection

from typing import List, Dict, Optional, Union


class FastSQL:

    def __init__(self, url: str, port: int, database: str, user: str, password: str) -> None:
        self.url: str = url
        self.port: int = port
        self.database: str = database
        self.user: str = user
        self.password: str = password
        self.connection: Optional[Connection] = None

    def execute(self, query: str, data: Dict[str, object], column_collapse: bool = False, row_collapse: bool = False) -> Union[Dict[str, object], List[Dict[str, object]], List[object], object, None]:

        with self.connection.cursor() as cursor:
            cursor.execute(query, data)
            rows = cursor.fetchall()

            if len(rows) == 0:
                return None

            elif len(rows) == 1 and row_collapse:
                if len(cursor.description) == 1 and column_collapse:
                    return rows[0][0]
                else:
                    return {item.name: rows[0][index] for index, item in enumerate(cursor.description)}
            else:
                if len(cursor.description) == 1 and column_collapse:
                    return [row[0] for row in rows]
                else:
                    return [{item.name: row[index] for index, item in enumerate(cursor.description)} for row in rows]

    def start(self) -> None:
        self.connection = connect(dbname=self.database, user=self.user, password=self.password, host=self.url, port=self.port)

    def stop(self) -> None:
        self.connection.close()
        self.connection.close()

    def include_router(self, router: 'SQLRouter') -> None:
        router.include(self)


class SQLRouter:

    def __init__(self) -> None:
        self.client: FastSQL = None
    
    def include(self, client: FastSQL) -> None:
        self.client = client

    def execute(self, query: str, data: Dict[str, object], column_collapse: bool = False, row_collapse: bool = False) -> Union[Dict[str, object], List[Dict[str, object]], List[object], object, None]:
        self.client.execute(query, data, column_collapse, row_collapse)
