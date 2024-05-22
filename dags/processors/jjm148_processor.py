import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

class Jjm148Processor:
    def __init__(self, postgres_conn_id, api_url):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.api_url = api_url
        self.logger = logging.getLogger("jjm148_processor")

    def fetch_data(self, api_url):
        self.logger.info(f"Fetching data from {api_url}")
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        self.logger.info(f"Fetched data: {data}")
        return data

    def check_and_create_tables(self):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            tables = [
                {
                    "name": "jjm148_aqi",
                    "create_statement": """
                        CREATE TABLE IF NOT EXISTS jjm148_aqi (
                            id SERIAL PRIMARY KEY,
                            RecordedDate DATE,
                            RecordedTime TIME,
                            StationCode INT,
                            StationName TEXT,
                            MonitorTypeCode INT,
                            MonitorTypeName TEXT,
                            ObsValue REAL,
                            MeasurementUnit TEXT,
                            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            owner TEXT
                        )
                    """
                },
                {
                    "name": "jjm148_CL_Stations",
                    "create_statement": """
                        CREATE TABLE IF NOT EXISTS jjm148_CL_Stations (
                            id SERIAL PRIMARY KEY,
                            StationCode INT,
                            StationName TEXT,
                            StationLongName TEXT,
                            City TEXT,
                            StationAddress TEXT,
                            StationLatitude REAL,
                            StationLongitude REAL,
                            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            owner TEXT
                        )
                    """
                },
                {
                    "name": "jjm148_CL_MonitorTypes",
                    "create_statement": """
                        CREATE TABLE IF NOT EXISTS jjm148_CL_MonitorTypes (
                            id SERIAL PRIMARY KEY,
                            MonitorTypeCode INT,
                            MonitorTypeDescription TEXT,
                            MonitorTypeName TEXT,
                            MonitorTypeUnits TEXT,
                            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            owner TEXT
                        )
                    """
                }
            ]
            for table in tables:
                self.logger.info(f"Checking and creating table {table['name']}")
                cursor.execute(table["create_statement"])
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error creating tables: {e}")
        finally:
            cursor.close()
            conn.close()

    def insert_items(self, items, owner, table_name):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            count = 0
            duplicate_count = 0
            for item in items:
                # Prepare SQL based on table name
                if table_name == 'jjm148_aqi':
                    cursor.execute("""
                        SELECT 1 FROM jjm148_aqi WHERE 
                            RecordedDate = %s AND 
                            RecordedTime = %s AND 
                            StationCode = %s AND 
                            MonitorTypeCode = %s
                    """, (
                        item['RecordedDate'],
                        item['RecordedTime'],
                        item['StationCode'],
                        item['MonitorTypeCode']
                    ))

                    if cursor.fetchone():
                        duplicate_count += 1
                    else:
                        cursor.execute("""
                            INSERT INTO jjm148_aqi (
                                RecordedDate, RecordedTime, StationCode, StationName, 
                                MonitorTypeCode, MonitorTypeName, ObsValue, MeasurementUnit, owner
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            item['RecordedDate'],
                            item['RecordedTime'],
                            item['StationCode'],
                            item['StationName'],
                            item['MonitorTypeCode'],
                            item['MonitorTypeName'],
                            item['ObsValue'],
                            item['MeasurementUnit'],
                            owner
                        ))
                        count += 1
                elif table_name == 'jjm148_CL_Stations':
                    cursor.execute("""
                        SELECT 1 FROM jjm148_CL_Stations WHERE 
                            StationCode = %s
                    """, (item['StationCode'],))

                    if cursor.fetchone():
                        duplicate_count += 1
                    else:
                        cursor.execute("""
                            INSERT INTO jjm148_CL_Stations (
                                StationCode, StationName, StationLongName, City, 
                                StationAddress, StationLatitude, StationLongitude, owner
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            item['StationCode'],
                            item['StationName'],
                            item['StationLongName'],
                            item['City'],
                            item['StationAddress'],
                            item['StationLatitude'],
                            item['StationLongitude'],
                            owner
                        ))
                        count += 1
                elif table_name == 'jjm148_CL_MonitorTypes':
                    cursor.execute("""
                        SELECT 1 FROM jjm148_CL_MonitorTypes WHERE 
                            MonitorTypeCode = %s
                    """, (item['MonitorTypeCode'],))

                    if cursor.fetchone():
                        duplicate_count += 1
                    else:
                        cursor.execute("""
                            INSERT INTO jjm148_CL_MonitorTypes (
                                MonitorTypeCode, MonitorTypeDescription, MonitorTypeName, MonitorTypeUnits, owner
                            ) VALUES (%s, %s, %s, %s, %s)
                        """, (
                            item['MonitorTypeCode'],
                            item['MonitorTypeDescription'],
                            item['MonitorTypeName'],
                            item['MonitorTypeUnits'],
                            owner
                        ))
                        count += 1

            conn.commit()
            self.logger.info(f"{count} items inserted into {table_name}.")
            self.logger.info(f"{duplicate_count} items were duplicates and not inserted into {table_name}.")
        except Exception as e:
            self.logger.error(f"Error inserting items into {table_name}: {e}")
        finally:
            cursor.close()
            conn.close()
