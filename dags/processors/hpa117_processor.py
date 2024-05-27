import logging
from datetime import datetime

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook


class Hpa117Processor:
    def __init__(self, postgres_conn_id, api_url):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.api_url = api_url
        self.logger = logging.getLogger("hpa117_processor")

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
            self.logger.info("Checking and creating table hpa117_rainfall")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hpa117_rainfall (
                    id SERIAL PRIMARY KEY,
                    RainToday INT,
                    api_id UUID,
                    Site_Name TEXT,
                    Last_Sample TIMESTAMP,
                    SITE_NO INT,
                    Sub_text TEXT,
                    created_at TIMESTAMP,
                    Last_Hour REAL,
                    Total_Rainfall REAL,
                    ShortName TEXT,
                    SiteOwner TEXT,
                    updated_at TIMESTAMP,
                    status BOOLEAN,
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
                    SELECT 1 FROM hpa117_rainfall WHERE 
                        api_id = %s
                """, (item['id'],))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                        INSERT INTO hpa117_rainfall (
                            RainToday, api_id, Site_Name, Last_Sample, SITE_NO, Sub_text,
                            created_at, Last_Hour, Total_Rainfall, ShortName, SiteOwner,
                            updated_at, status, owner
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        item['RainToday'],
                        item['id'],
                        item['Site_Name'],
                        datetime.strptime(item['Last_Sample'], '%d/%m/%Y %I:%M:%S %p'),
                        item['SITE_NO'],
                        item['Sub_text'],
                        datetime.strptime(item['created_at'], '%Y-%m-%dT%H:%M:%S.%f+00:00'),
                        item['Last_Hour'],
                        item['Total_Rainfall'],
                        item['ShortName'],
                        item['SiteOwner'],
                        datetime.strptime(item['updated_at'], '%Y-%m-%dT%H:%M:%S.%f+00:00') if item[
                            'updated_at'] else None,
                        item['status'],
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
