import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

class Hwa205Processor:
    def __init__(self, postgres_conn_id, api_url):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.api_url = api_url
        self.logger = logging.getLogger("hwa205_processor")

    def fetch_data(self):
        self.logger.info(f"Fetching data from {self.api_url}")
        query = '''
        {
            stationItems {
                StationCode
                StationName
                StationShortName
                StationLocation
                StationCity
                StationLatitude
                StationLongitude
                MonitorChannel
                MonitorName
                MonitorTypeCode
                MonitorTypeDescription
                MonitorFullName
            }
        }
        '''
        response = requests.post(self.api_url, json={'query': query})
        response.raise_for_status()
        data = response.json()
        self.logger.info(f"Fetched data: {data}")
        return data['data']['stationItems']

    def check_and_create_table(self):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hua_aqi_stations (
                    id SERIAL PRIMARY KEY,
                    StationCode TEXT,
                    StationName TEXT,
                    StationShortName TEXT,
                    StationLocation TEXT,
                    StationCity TEXT,
                    StationLatitude REAL,
                    StationLongitude REAL,
                    MonitorChannel TEXT,
                    MonitorName TEXT,
                    MonitorTypeCode TEXT,
                    MonitorTypeDescription TEXT,
                    MonitorFullName TEXT,
                    insertTimestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
                    SELECT 1 FROM hua_aqi_stations WHERE 
                        StationCode = %s AND 
                        StationName = %s AND 
                        StationShortName = %s AND 
                        StationLocation = %s AND 
                        StationCity = %s AND 
                        StationLatitude = %s AND 
                        StationLongitude = %s AND 
                        MonitorChannel = %s AND 
                        MonitorName = %s AND 
                        MonitorTypeCode = %s AND 
                        MonitorTypeDescription = %s AND 
                        MonitorFullName = %s
                """, (
                    item['StationCode'],
                    item['StationName'],
                    item['StationShortName'],
                    item['StationLocation'],
                    item['StationCity'],
                    item['StationLatitude'],
                    item['StationLongitude'],
                    item['MonitorChannel'],
                    item['MonitorName'],
                    item['MonitorTypeCode'],
                    item['MonitorTypeDescription'],
                    item['MonitorFullName'],
                ))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                        INSERT INTO hua_aqi_stations (
                            StationCode, StationName, StationShortName, StationLocation, StationCity,
                            StationLatitude, StationLongitude, MonitorChannel, MonitorName,
                            MonitorTypeCode, MonitorTypeDescription, MonitorFullName, owner
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        item['StationCode'],
                        item['StationName'],
                        item['StationShortName'],
                        item['StationLocation'],
                        item['StationCity'],
                        item['StationLatitude'],
                        item['StationLongitude'],
                        item['MonitorChannel'],
                        item['MonitorName'],
                        item['MonitorTypeCode'],
                        item['MonitorTypeDescription'],
                        item['MonitorFullName'],
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
