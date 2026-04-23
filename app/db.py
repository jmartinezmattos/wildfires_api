import os
import aiomysql
import os
from dotenv import load_dotenv
from datetime import date

from app.schemas import FireRevision

load_dotenv("config/.env")

MYSQL_FIRMS_TABLE = os.getenv("MYSQL_FIRMS_TABLE")
MYSQL_METRICS_TABLE = os.getenv("MYSQL_METRICS_TABLE")
MYSQL_BATCH_TABLE = os.getenv("MYSQL_BATCH_TABLE")
MYSQL_WMS_DATETIME_TABLE = os.getenv("MYSQL_WMS_DATETIME_TABLE")

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

    async def fetch_unchecked_fires(
    self,
    limit: int = 10,
    start_date: date | None = None,
    end_date: date | None = None,
    source: str | None = None,
):  
        print(f"LIMIT: {limit}, START_DATE: {start_date}, END_DATE: {end_date}, SOURCE: {source}")
        normalized_source = source.lower() if source else None
        
        # 1. Define the sub-queries
        firms_query = f"""
            SELECT
                id,
                gcs_image_path,
                'firms' AS source,
                firms_datetime AS timestamp
            FROM {MYSQL_FIRMS_TABLE}
            WHERE revised IS FALSE AND prediction = 'Fire'
        """
        
        batch_query = f"""
            SELECT
                id,
                gcs_path_rgb AS gcs_image_path,
                'batch' AS source,
                timestamp_utc AS timestamp
            FROM {MYSQL_BATCH_TABLE}
            WHERE revised IS FALSE AN prediction_multiband = 'Fire'
        """

        totals_query = f"""
            SELECT
                (
                    SELECT COUNT(*)
                    FROM {MYSQL_FIRMS_TABLE}
                    WHERE revised IS FALSE AND prediction = 'Fire'
                ) AS total_unchecked_firms,
                (
                    SELECT COUNT(*)
                    FROM {MYSQL_BATCH_TABLE}
                    WHERE revised IS FALSE AND (prediction_rgb = 'Fire' OR prediction_multiband = 'Fire')
                ) AS total_unchecked_batch
        """

        # 2. Decide which parts to include based on 'source'
        if normalized_source == 'firms':
            base_sql = firms_query
        elif normalized_source == 'batch':
            base_sql = batch_query
        else:
            # If None or something else, combine both
            base_sql = f"({firms_query}) UNION ALL ({batch_query})"

        # 3. Wrap and add the shared filters
        final_sql = f"""
            SELECT * FROM ({base_sql}) AS combined
            WHERE (%s IS NULL OR DATE(timestamp) >= %s)
            AND (%s IS NULL OR DATE(timestamp) <= %s)
            ORDER BY timestamp DESC
            LIMIT %s;
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(totals_query)
                totals = await cursor.fetchone() or {
                    "total_unchecked_firms": 0,
                    "total_unchecked_batch": 0,
                }

                await cursor.execute(
                    final_sql,
                    (start_date, 
                     start_date, 
                     end_date,
                     end_date, 
                     limit),
                )
                rows = await cursor.fetchall()

                return {
                    "fires": rows,
                    **totals,
                }
        

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




    @staticmethod
    def _build_firms_subquery(start_date, end_date, confirmed_only=False,):
    
        sql = f"""
            SELECT id, latitude, longitude, acq_date, gcs_image_path, fwi_category as fwi, 'FIRMS' AS source, revised
            FROM {MYSQL_FIRMS_TABLE}
            WHERE firms_datetime BETWEEN %s AND %s
            AND prediction = 'Fire'


        """
        params = [start_date, end_date]

        if confirmed_only:
            sql += " AND revised = TRUE AND is_fire = TRUE"

        return sql, params
    

    @staticmethod
    def _build_batch_subquery(start_date, end_date, confirmed_only=False,):
    # Currently consider fire those classified as such by both RGB and multiband models.
        sql = f"""
            SELECT id, lat_center AS latitude, lon_center AS longitude, timestamp_utc AS acq_date, gcs_path_rgb AS gcs_image_path, fwi_category AS fwi, 'BATCH' AS source, revised
            FROM {MYSQL_BATCH_TABLE}
            WHERE timestamp_utc BETWEEN %s AND %s
            AND prediction_rgb = 'Fire'
            And prediction_multiband = 'Fire' 

        """
        params = [start_date, end_date]

        if confirmed_only:
            sql += " AND revised = TRUE AND is_fire = TRUE"

        return sql, params


    async def fetch_fires(self, start_date, end_date, source=None, confirmed_only=False):
        queries = []
        params = []

        if source in ("ALL", "FIRMS"):
            q, p = self._build_firms_subquery(start_date, end_date, confirmed_only)
            queries.append(q)
            params.extend(p)

        if source in ("ALL", "BATCH"):
            q, p = self._build_batch_subquery(start_date, end_date, confirmed_only)
            queries.append(q)
            params.extend(p)

        sql = " UNION ALL ".join(queries)

        print(f"SQL is {sql}")
        print(f"Params are {params}")


        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(sql, params)
                rows = await cursor.fetchall()

        return rows
    
    async def fetch_firms_alerts(self, start_date, end_date):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:

                sql = f"""
                    SELECT id, latitude, longitude, acq_date
                    FROM {MYSQL_FIRMS_TABLE}
                    WHERE firms_datetime BETWEEN %s AND %s
                """
                await cursor.execute(sql, (start_date, end_date))
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
                await cursor.execute(sql, (metric_name))
                row = await cursor.fetchone()

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

    async def fetch_wms_datetimes(self):
        product_to_key = {
            "NDVI_GRANULES": "ndvi",
            "LST_GRANULES": "lst",
            "TRUE_COLOR": "true_color",
        }
        grouped_datetimes = {"lst": [], "ndvi": [], "true_color": []}

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                sql = f"""
                    SELECT product, datetime
                    FROM {MYSQL_WMS_DATETIME_TABLE}
                    WHERE product IN ('NDVI_GRANULES', 'LST_GRANULES', 'TRUE_COLOR')
                    ORDER BY datetime DESC
                """
                await cursor.execute(sql)
                rows = await cursor.fetchall()

        for row in rows:
            key = product_to_key.get(row.get("product"))
            value = row.get("datetime")

            if key and value:
                grouped_datetimes[key].append(value)

        return grouped_datetimes


    

        
db_client = CloudSQLClient(DB_CONFIG)
