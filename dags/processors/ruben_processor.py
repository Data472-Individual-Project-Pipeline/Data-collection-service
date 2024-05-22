import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

class RubenProcessor:
    def __init__(self, postgres_conn_id, api_url):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.api_url = api_url
        self.logger = logging.getLogger("ruben_processor")

    def fetch_data(self):
        self.logger.info(f"Fetching data from {self.api_url}")
        response = requests.get(self.api_url)
        response.raise_for_status()
        data = response.json()
        self.logger.info(f"Fetched data: {data}")
        return data

    def check_and_create_table(self):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            self.logger.info("Checking and creating table ruben_employment_indicators")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ruben_employment_indicators (
                    id SERIAL PRIMARY KEY,
                    Duration TEXT,
                    Geo TEXT,
                    GeoUnit TEXT,
                    api_id UUID,
                    Label1 TEXT,
                    Label2 TEXT,
                    Measure TEXT,
                    Multiplier INT,
                    NullReason TEXT,
                    Period DATE,
                    Unit TEXT,
                    Value REAL,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT
                )
            """)
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error creating table: {e}")
        finally:
            cursor.close()
            conn.close()

    def insert_items(self, items, owner):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            count = 0
            duplicate_count = 0
            for item in items:
                cursor.execute("""
                    SELECT 1 FROM ruben_employment_indicators WHERE 
                        api_id = %s
                """, (item['ID'],))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                        INSERT INTO ruben_employment_indicators (
                            Duration, Geo, GeoUnit, api_id, Label1, Label2, Measure, 
                            Multiplier, NullReason, Period, Unit, Value, owner
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        item['Duration'],
                        item['Geo'],
                        item['GeoUnit'],
                        item['ID'],
                        item['Label1'],
                        item['Label2'],
                        item['Measure'],
                        item['Multiplier'],
                        item['NullReason'],
                        item['Period'],
                        item['Unit'],
                        item['Value'],
                        owner
                    ))
                    count += 1
            conn.commit()
            self.logger.info(f"{count} items inserted.")
            self.logger.info(f"{duplicate_count} items were duplicates and not inserted.")
        except Exception as e:
            self.logger.error(f"Error inserting items: {e}")
        finally:
            cursor.close()
            conn.close()
