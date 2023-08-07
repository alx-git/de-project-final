import os
import vertica_python

from py.lib.vertica_connect import VerticaConnect


class DwhLoader:

    def __init__(self, db: VerticaConnect) -> None: 
        self._db = db 

    def global_metrics_loader(self, latest_update_dt, current_update_dt):
        connection = self._db.connection()
        if connection is None:
            return
        try:
            cursor = connection.cursor()
            with open('/lessons/dags/../py/dwh/load_global_metrics.sql', 'r') as sql_file:
                sql_commands = sql_file.read()
            cursor.execute(sql_commands,
                           {'latest_update_dt': latest_update_dt,
                            'current_update_dt': current_update_dt
                           })
            connection.commit()
            cursor.close()
            connection.close()
        except vertica_python.Error as e:
            print("Error executing the query:", e)
            connection.close()
