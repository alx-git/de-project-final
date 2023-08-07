from datetime import datetime
from pydantic import BaseModel
from vertica_python import Connection


class TimeSettings(BaseModel):
    database: str
    update_ts: datetime


class StgTimeSettingsRepository:
    def get_setting(self, conn: Connection, database: str):
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT update_ts
                    FROM KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.time_settings
                    WHERE database = :database;
                """,
                {"database": database}
            )
            obj = cur.fetchone()
        return obj

    def save_setting(self, conn: Connection, database: str, update_ts: datetime) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    MERGE INTO KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.time_settings dest
                    USING (select :database as database, 
                    cast(:update_ts as timestamp) as update_ts) src
                    ON src.database = dest.database
                    WHEN MATCHED THEN
                    UPDATE SET update_ts = src.update_ts
                    WHEN NOT MATCHED THEN
                    INSERT (database, update_ts) VALUES (src.database, src.update_ts);
                """,
                {
                    "database": database,
                    "update_ts": update_ts
                }
            )
            conn.commit()
            cur.close()
        conn.close()