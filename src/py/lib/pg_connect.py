import psycopg

from airflow.hooks.base import BaseHook


class PgConnect: 
    def __init__(self, host: str, port: int, db_name: str,
                 user: str, pw: str, sslmode: str = "require") -> None: 
        self.host = host 
        self.port = port 
        self.db_name = db_name 
        self.user = user 
        self.pw = pw 
        self.sslmode = sslmode 
 
    def connection(self):
        try:
            host = self.host
            port = self.port
            database = self.db_name
            user = self.user
            password = self.pw
            sslmode = self.sslmode

            connection = psycopg.connect(
                dbname=database,
                user=user,
                password=password,
                host=host,
                port=port,
                sslmode=sslmode
            )

            return connection

        except psycopg.Error as e:
            print("Error connecting to the database:", e)
            return None


class PgConnectionBuilder:

    @staticmethod
    def pg_conn(conn_id: str) -> PgConnect:
        conn = BaseHook.get_connection(conn_id)

        sslmode = "require"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]

        pg = PgConnect(str(conn.host),
                       str(conn.port),
                       str(conn.schema),
                       str(conn.login),
                       str(conn.password),
                       sslmode)

        return pg
