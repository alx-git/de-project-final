import psycopg
from psycopg.rows import class_row

from py.lib.pg_connect import PgConnect
from py.stg.stg_objects import transactions, currencies 
 
 
class StgReader:

    def __init__(self, db: PgConnect) -> None:
        self._db = db 
    
    def transactions_reader(self, latest_update_dt, current_update_dt) -> transactions:
        connection = self._db.connection()
        if connection is None:
            return
        try:
            cursor = connection.cursor(row_factory=class_row(transactions))
            cursor.execute(
                    """ 
                        select * from public.transactions
                        where transaction_dt > %(latest_update_dt)s
                        and transaction_dt < %(current_update_dt)s; 
                    """,
                    {
                        'latest_update_dt': latest_update_dt,
                        'current_update_dt': current_update_dt
                    }
            )
            rows = cursor.fetchall()
            cursor.close()
            connection.close()
            return rows
        except psycopg.Error as e:
            print("Error executing the query:", e)
            connection.close()

    def currencies_reader(self, latest_update_dt, current_update_dt) -> currencies:
        connection = self._db.connection()
        if connection is None:
            return
        try:
            cursor = connection.cursor(row_factory=class_row(currencies))
            cursor.execute(
                    """ 
                        select * from public.currencies
                        where date_update > %(latest_update_dt)s
                        and date_update < %(current_update_dt)s; 
                    """,
                    {
                        'latest_update_dt': latest_update_dt,
                        'current_update_dt': current_update_dt
                    }
            )
            rows = cursor.fetchall()
            cursor.close()
            connection.close()
            return rows
        except psycopg.Error as e:
            print("Error executing the query:", e)
            connection.close()
