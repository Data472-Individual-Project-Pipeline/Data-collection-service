import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

class Vsu37Processor:
    def __init__(self, postgres_conn_id, api_url):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.api_url = api_url
        self.logger = logging.getLogger("vsu37_processor")

    def fetch_data(self):
        self.logger.info(f"Fetching data from {self.api_url}")
        response = requests.get(self.api_url)
        response.raise_for_status()
        data = response.json()
        self.logger.info(f"Fetched data: {data}")
        return data['data']['item']

    def check_and_create_table(self):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            self.logger.info("Checking and creating table vsu37_rainfall")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vsu37_rainfall (
                    id SERIAL PRIMARY KEY,
                    last_hour REAL,
                    last_sample TIMESTAMP,
                    location TEXT,
                    owner_logo TEXT,
                    rain_today REAL,
                    site_no TEXT,
                    short_name TEXT,
                    site_owner TEXT,
                    site_name TEXT,
                    total_rainfall REAL,
                    latitude REAL,
                    longitude REAL,
                    day1 REAL,
                    day2 REAL,
                    day3 REAL,
                    day4 REAL,
                    day5 REAL,
                    day6 REAL,
                    day7 REAL,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error creating table: {e}")
        finally:
            cursor.close()
            conn.close()

    def insert_items(self, items):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            count = 0
            duplicate_count = 0
            for item in items:
                cursor.execute("""
                    SELECT 1 FROM vsu37_rainfall WHERE 
                        site_no = %s AND 
                        last_sample = %s
                """, (
                    item.get('SITE_NO', None),
                    item.get('Last_x0020_Sample', None)
                ))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                        INSERT INTO vsu37_rainfall (
                            last_hour, last_sample, location, owner_logo, rain_today, 
                            site_no, short_name, site_owner, site_name, total_rainfall, 
                            latitude, longitude, day1, day2, day3, day4, day5, day6, day7
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        float(item.get('Last_x0020_Hour', 0.0)),
                        item.get('Last_x0020_Sample', None),
                        item.get('Location', ''),
                        item.get('OwnerLogo', ''),
                        float(item.get('RainToday', 0.0)),
                        item.get('SITE_NO', ''),
                        item.get('ShortName', ''),
                        item.get('SiteOwner', ''),
                        item.get('Site_x0020_Name', ''),
                        float(item.get('Total_x0020_Rainfall', 0.0)),
                        float(item.get('WGS84_Latitude', 0.0)),
                        float(item.get('WGS84_Longitude', 0.0)),
                        float(item.get('_x002D_1_x0020_Day', 0.0)),
                        float(item.get('_x002D_2_x0020_Day', 0.0)),
                        float(item.get('_x002D_3_x0020_Day', 0.0)),
                        float(item.get('_x002D_4_x0020_Day', 0.0)),
                        float(item.get('_x002D_5_x0020_Day', 0.0)),
                        float(item.get('_x002D_6_x0020_Day', 0.0)),
                        float(item.get('_x002D_7_x0020_Day', 0.0))
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
