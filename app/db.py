import os
import aiomysql
import os
from dotenv import load_dotenv

from app.schemas import FireRevision

load_dotenv("config/.env")

MYSQL_FIRMS_TABLE = os.getenv("MYSQL_FIRMS_TABLE")
MYSQL_METRICS_TABLE = os.getenv("MYSQL_METRICS_TABLE")
MYSQL_BATCH_TABLE = os.getenv("MYSQL_BATCH_TABLE")

DB_CONFIG = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DB"),
    "connection_name": os.getenv("MYSQL_CONNECTION_NAME"),
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
}

class CloudSQLClient:
    def __init__(self, db_config):
        self.db_config = db_config
        self.pool = None

    async def connect(self):
        if self.db_config.get("connection_name"):

            self.pool = await aiomysql.create_pool(
                user=self.db_config["user"],
                password=self.db_config["password"],
                db=self.db_config["database"],
                unix_socket=f"/cloudsql/{self.db_config['connection_name']}",
                autocommit=True,
            )
        else:

            self.pool = await aiomysql.create_pool(
                host=self.db_config["host"],
                port=self.db_config["port"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                db=self.db_config["database"],
                autocommit=True,
            )

    async def fetch_unchecked_fires(self, limit: int = 100):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:

                sql = f"""
                    SELECT * FROM (
                        SELECT id, gcs_image_path, 'firms' AS source, firms_datetime AS timestamp
                        FROM {MYSQL_FIRMS_TABLE}
                        WHERE revised is FALSE
                        AND prediction = 'Fire'
                        
                        UNION ALL

                        SELECT id, gcs_path AS gcs_image_path, 'batch' AS source, timestamp_utc AS timestamp
                        FROM {MYSQL_BATCH_TABLE}
                        WHERE revised IS FALSE
                    ) AS combined_results
                    LIMIT {limit};
                    """ 

                await cursor.execute(sql)
                rows = await cursor.fetchall()

        return rows
    

    @staticmethod
    def _is_fire_query(is_fire_query_string: str, table_name: str, ids: list):
        # If no IDs were added for this table, return None to avoid a crash
        if not ids:
            return None
            
        # Crucial: Wrap the IDs in the WHERE clause with single quotes
        # and ensure Boolean True/False is converted to 1/0 for MySQL
        where_ids = ", ".join([f"'{id}'" for id in ids])
        
        final_query = f"""
            UPDATE {table_name}
            SET is_fire = CASE id
                {is_fire_query_string}
            END,
            revised = TRUE
            WHERE id IN ({where_ids});
        """
        return final_query

    async def process_revision(self, revisions: list[FireRevision]):
        firms_cases = []
        batch_cases = []
        firms_ids = []
        batch_ids = []

        for rev in revisions:
            # We wrap the ID in quotes and convert Boolean to integer (1 or 0)
            case_line = f"WHEN '{rev.id}' THEN {int(rev.is_fire)}"
            
            if rev.source == "firms":
                firms_cases.append(case_line)
                firms_ids.append(rev.id)
            elif rev.source == "batch":
                batch_cases.append(case_line)
                batch_ids.append(rev.id)

        # Join the lines into a single string
        firms_query = self._is_fire_query("\n".join(firms_cases), MYSQL_FIRMS_TABLE, firms_ids)
        batch_query = self._is_fire_query("\n".join(batch_cases), MYSQL_BATCH_TABLE, batch_ids)

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                if firms_query:
                    await cursor.execute(firms_query)
                if batch_query:
                    await cursor.execute(batch_query)
            await conn.commit()



    async def fetch_fires(self, start_date, end_date):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                #sql = f"""
                #    SELECT id, latitude, longitude, acq_date, gcs_image_path
                #    FROM {MYSQL_FIRMS_TABLE}
                #    WHERE firms_datetime BETWEEN %s AND %s
                #    AND prediction = 'Fire'
                #"""

                sql = f"""
                    SELECT id, latitude, longitude, acq_date, gcs_image_path, fwi_category as fwi
                    FROM {MYSQL_FIRMS_TABLE}
                    WHERE firms_datetime BETWEEN %s AND %s
                    AND prediction = 'Fire'
                    
                    UNION ALL

                    SELECT id, lat_center AS latitude, lon_center AS longitude, timestamp_utc AS acq_date, gcs_path AS gcs_image_path, fwi_category AS fwi
                    FROM {MYSQL_BATCH_TABLE}
                    WHERE timestamp_utc BETWEEN %s AND %s
                """

                await cursor.execute(sql, (start_date, end_date, start_date, end_date))
                rows = await cursor.fetchall()

        return rows
    
    async def fetch_metric(self, date, metric_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                sql = f"""
                    SELECT *
                    FROM {MYSQL_METRICS_TABLE}
                    WHERE acq_datetime >= %s
                    AND acq_datetime < DATE_ADD(%s, INTERVAL 1 DAY)
                    AND metric = %s
                    ORDER BY acq_datetime DESC
                    LIMIT 1
                """
                await cursor.execute(sql, (date, date, metric_name))
                row = await cursor.fetchone()

        return row
    
    async def fetch_last_metric(self, metric_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                sql = f"""
                    SELECT *
                    FROM {MYSQL_METRICS_TABLE}
                    WHERE metric = %s
                    ORDER BY acq_datetime DESC
                    LIMIT 1
                """
                print(sql)
                await cursor.execute(sql, (metric_name))
                row = await cursor.fetchone()
                print(row)


        return row
    
    async def fetch_metric_by_date(self, metric_name: str, acq_date):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                sql = f"""
                    SELECT *
                    FROM {MYSQL_METRICS_TABLE}
                    WHERE metric = %s
                    AND acq_datetime >= %s
                    AND acq_datetime < DATE_ADD(%s, INTERVAL 1 DAY)
                    ORDER BY acq_datetime DESC
                    LIMIT 1
                """
                await cursor.execute(sql, (metric_name, acq_date, acq_date))
                row = await cursor.fetchone()

        return row


    

        
db_client = CloudSQLClient(DB_CONFIG)
