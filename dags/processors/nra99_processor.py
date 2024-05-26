import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


class Nra99Processor:
    def __init__(self, postgres_conn_id, api_url):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.api_url = api_url
        self.logger = logging.getLogger("nra99_processor")

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
            self.logger.info("Checking and creating table nra99_visi")
            cursor.execute("""
                    CREATE TABLE IF NOT EXISTS nra99_visi (
                        id SERIAL PRIMARY KEY,
                        Year DATE,
                        Month DATE,
                        CountryOfResidence TEXT,
                        Purpose TEXT,
                        NZPort TEXT,
                        NumberOfPeople INT,
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
                        SELECT 1 FROM nra99_visi WHERE
                            Year = %s AND,
                            Month = %s AND,
                            CountryOfResidence = %s AND,
                            Purpose = %s AND,
                            NZPort = %s AND,
                            NumberOfPeople = %s
                    """, (
                    item['Year'],
                    item['Month'],
                    item['Country of Residence'],
                    item['Purpose'],
                    item['NZ Port'],
                    item['Number of People']
                ))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                                           INSERT INTO nra99_visi (
                                               Year, Month, CountryOfResidence, Purpose, NZPort, NumberOfPeople
                                           ) VALUES (%s, %s, %s, %s, %s, %s)
                                       """, (
                        item['Year'],
                        item['Month'],
                        item['Country of Residence'],
                        item['Purpose'],
                        item['NZ Port'],
                        item['Number of People'],
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
