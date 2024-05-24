import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

class Are154Processor:
    def __init__(self, postgres_conn_id, api_url):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.api_url = api_url
        self.logger = logging.getLogger("are154_processor")

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
            self.logger.info("Checking and creating table are154_data")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS are154_data (
                    id SERIAL PRIMARY KEY,
                    Date DATE,
                    Observation INT,
                    RecipientCharacteristic TEXT,
                    Subtype TEXT,
                    Type TEXT,
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

    def format_date(self, date_str):
        # 将日期从 YYYY-MM-DD 格式转换为 YYYY-MM-DD 格式
        return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')

    def insert_items(self, items, owner):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            count = 0
            duplicate_count = 0
            for item in items:
                formatted_date = self.format_date(item['Date'])  # 日期已经是字符串形式
                cursor.execute("""
                    SELECT 1 FROM are154_data WHERE 
                        Date = %s AND 
                        RecipientCharacteristic = %s AND 
                        Subtype = %s AND 
                        Type = %s
                """, (
                    formatted_date,
                    item['Recipient Characteristic'],
                    item['Subtype'],
                    item['Type']
                ))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                        INSERT INTO are154_data (
                            Date, Observation, RecipientCharacteristic, Subtype, Type, owner
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        formatted_date,
                        item['Observation'],
                        item['Recipient Characteristic'],
                        item['Subtype'],
                        item['Type'],
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
