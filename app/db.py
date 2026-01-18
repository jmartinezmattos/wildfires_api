import os
import mysql.connector
from app.utils import serialize_row, convert_to_geojson, generate_signed_url

DB_CONFIG = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DB"),
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "connection_name": os.getenv("MYSQL_CONNECTION_NAME"),
}


class CloudSQLClient:
    def __init__(self, db_config):
        if db_config.get("connection_name"):
            self.config = {
                "user": db_config["user"],
                "password": db_config["password"],
                "database": db_config["database"],
                "unix_socket": f"/cloudsql/{db_config['connection_name']}",
            }
        else:
            self.config = {
                "user": db_config["user"],
                "password": db_config["password"],
                "database": db_config["database"],
                "host": db_config["host"],
                "port": int(db_config["port"]),
            }

    def _get_connection(self):
        return mysql.connector.connect(**self.config)

    def fetch_table_to_geojson(self, table: str):
        conn = self._get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        serialized = [serialize_row(r) for r in rows]

        for row in serialized:
            row["signed_gcs_url"] = generate_signed_url(row.get("gcs_path"))

        return convert_to_geojson(serialized)

    def fetch_between_dates(self, table, start_date, end_date, fire=False):
        conn = self._get_connection()
        cursor = conn.cursor(dictionary=True)

        sql = f"""
            SELECT * FROM {table}
            WHERE firms_datetime BETWEEN %s AND %s
        """
        params = [start_date, end_date]

        if fire:
            sql += " AND prediction = %s"
            params.append("Fire")

        sql += " ORDER BY firms_datetime ASC"

        cursor.execute(sql, params)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        serialized = [serialize_row(r) for r in rows]
        return convert_to_geojson(serialized)


# instancia global reutilizable
db_client = CloudSQLClient(DB_CONFIG)
