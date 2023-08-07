import json
import vertica_python

from datetime import datetime
from typing import Any

from py.lib.vertica_connect import VerticaConnect


class StgLoader:

    def __init__(self, db: VerticaConnect) -> None: 
        self._db = db 

    def serialize_obj_to_json(self, base_model_obj):
        serialized_data_list = [obj.dict() for obj in base_model_obj]
        for serialized_data in serialized_data_list: 
            for key, value in serialized_data.items():
                if isinstance(value, datetime):
                    serialized_data[key] = value.isoformat()
        json_data = json.dumps(serialized_data_list)
        return json_data

    def transactions_loader(self, obj: Any):
        connection = self._db.connection()
        if connection is None:
            return
        try:
            cursor = connection.cursor()
            json_data = self.serialize_obj_to_json(obj)
            copy_query = ("""COPY KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.transactions
                          FROM STDIN PARSER fjsonparser();""")
            cursor.copy(copy_query, json_data)
            cursor.close()
            connection.close()
        except vertica_python.Error as e:
            print("Error executing the query:", e)
            connection.close()
    
    def currencies_loader(self, obj: Any):
        connection = self._db.connection()
        if connection is None:
            return
        try:
            cursor = connection.cursor()
            json_data = self.serialize_obj_to_json(obj)
            copy_query = ("""COPY KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.currencies
                           FROM STDIN PARSER fjsonparser();""")
            cursor.copy(copy_query, json_data)
            cursor.close()
            connection.close()
        except vertica_python.Error as e:
            print("Error executing the query:", e)
            connection.close()