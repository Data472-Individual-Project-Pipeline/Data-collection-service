import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

class Rna104Processor:
    def __init__(self, postgres_conn_id, api_url):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.api_url = api_url
        self.logger = logging.getLogger("rna104_processor")

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
            self.logger.info("Checking and creating table rna104_cycleway")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rna104_cycleway (
                    id SERIAL PRIMARY KEY,
                    ServiceStatus TEXT,
                    MajorCyclewayName TEXT,
                    Type TEXT,
                    CreateDate TIMESTAMP,
                    LastEditDate TIMESTAMP,
                    Shape_Length REAL,
                    GeometryPath TEXT,
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
                    SELECT 1 FROM rna104_cycleway WHERE 
                        ServiceStatus = %s AND 
                        MajorCyclewayName = %s AND 
                        Type = %s AND 
                        CreateDate = %s AND 
                        LastEditDate = %s AND 
                        Shape_Length = %s AND 
                        GeometryPath = %s
                """, (
                    item['ServiceStatus'],
                    item['MajorCyclewayName'],
                    item['Type'],
                    item['CreateDate'],
                    item['LastEditDate'],
                    item['Shape__Length'],
                    item['GeometryPath']
                ))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                        INSERT INTO rna104_cycleway (
                            ServiceStatus, MajorCyclewayName, Type, CreateDate, 
                            LastEditDate, Shape_Length, GeometryPath, owner
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        item['ServiceStatus'],
                        item['MajorCyclewayName'],
                        item['Type'],
                        item['CreateDate'],
                        item['LastEditDate'],
                        item['Shape__Length'],
                        item['GeometryPath'],
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
