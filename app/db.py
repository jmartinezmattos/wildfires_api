import os
import aiomysql
import os
from dotenv import load_dotenv

load_dotenv("config/.env")

MYSQL_FIRMS_TABLE = os.getenv("MYSQL_FIRMS_TABLE")

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

    async def fetch_fires(self, start_date, end_date):
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                sql = f"""
                    SELECT *
                    FROM {MYSQL_FIRMS_TABLE}
                    WHERE firms_datetime BETWEEN %s AND %s
                    AND prediction = 'Fire'
                """
                await cursor.execute(sql, (start_date, end_date))
                rows = await cursor.fetchall()

        return rows
        
db_client = CloudSQLClient(DB_CONFIG)
