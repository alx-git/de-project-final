import vertica_python

from contextlib import contextmanager 
from typing import Generator
from vertica_python import Connection

from airflow.hooks.base import BaseHook
 
 
class VerticaConnect: 
    def __init__(self, host: str, port: int, db_name: str,
                 user: str, pw: str) -> None: 
        self.host = host 
        self.port = port 
        self.db_name = db_name 
        self.user = user 
        self.pw = pw 
         
    def connection(self):
        try:
            host = self.host
            port = self.port
            database = self.db_name
            user = self.user
            password = self.pw
        
            connection = vertica_python.connect(
                dbname=database,
                user=user,
                password=password,
                host=host,
                port=port
            )

            return connection

        except vertica_python.Error as e:
            print("Error connecting to the database:", e)
            return None

class VerticaConnectionBuilder:

    @staticmethod
    def connect(conn_id: str) -> VerticaConnect:
        conn = BaseHook.get_connection(conn_id)

        vertica = VerticaConnect(str(conn.host),
                       str(conn.port),
                       str(conn.schema),
                       str(conn.login),
                       str(conn.password))

        return vertica