import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


class Dus15Processor:
    def __init__(self, postgres_conn_id, api_url):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.api_url = api_url
        self.logger = logging.getLogger("dus15_processor")

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
            self.logger.info("Checking and creating table dus15_seat")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dus15_seat (
                    id SERIAL PRIMARY KEY,
                    PhotographURL TEXT,
                    SiteName TEXT,
                    Type TEXT,
                    lat REAL,
                    lon REAL,
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

    def execute_insert_items(self, items, table, unique_fields, table_fields, owner):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            count = 0
            duplicate_count = 0
            for item in items:  # item field
                # found an issue with unique values parsing the fields hopefully this means the code works
                unique_values = tuple(f'{item[field]}' for field in table_fields)
                cursor.execute(
                    f"""SELECT 1 FROM {table} WHERE {" AND ".join(f'{field} = %s' for field in unique_fields)}""",
                    unique_values)
                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    insert_values = tuple(item[field] for field in table_fields) + (owner,)
                    cursor.execute(f"""
                               INSERT INTO {table} ({", ".join(unique_fields)}, owner)
                               VALUES ({", ".join("%s" for _ in table_fields)}, %s)
                           """, insert_values)
                    count += 1
            conn.commit()
            self.logger.info(f"{count} items inserted into {table}.")
            self.logger.info(f"{duplicate_count} duplicate items not inserted into {table}.")
        except Exception as e:
            self.logger.error(f"Error inserting items into {table}: {e}")
        finally:
            cursor.close()
            conn.close()

    def _insert_seat(self, items, owner):
        self.execute_insert_items(
            items,
            table='dus15_seat',
            unique_fields=['PhotographURL', 'SiteName', 'Type', 'lat', 'lon'],
            table_fields=['PhotographURL', 'SiteName', 'Type', 'lat', 'lon'],
            owner=owner
        )

    def insert_items(self, item_data, owner):
        self._insert_seat(item_data, owner)

# def insert_items(self, items, owner):
#     conn = self.hook.get_conn()
#     cursor = conn.cursor()
#     try:
#         count = 0
#         duplicate_count = 0
#         for item in items:
#             cursor.execute("""
#                 SELECT 1 FROM dus15_seat WHERE
#                     PhotographURL = %s AND
#                     SiteName = %s AND
#                     Type = %s AND
#                     lat = %s AND
#                     lon = %s
#             """, (
#                 item['PhotographURL'],
#                 item['SiteName'],
#                 item['Type'],
#                 item['lat'],
#                 item['lon']
#             ))
#
#             if cursor.fetchone():
#                 duplicate_count += 1
#             else:
#                 cursor.execute("""
#                     INSERT INTO dus15_seat (
#                         PhotographURL, SiteName, Type, lat, lon, owner
#                     ) VALUES (%s, %s, %s, %s, %s, %s)
#                 """, (
#                     item['PhotographURL'],
#                     item['SiteName'],
#                     item['Type'],
#                     item['lat'],
#                     item['lon'],
#                     owner
#                 ))
#                 count += 1
#         conn.commit()
#         self.logger.info(f"{count} items inserted.")
#         self.logger.info(f"{duplicate_count} items were duplicates and not inserted.")
#     except Exception as e:
#         self.logger.error(f"Error inserting items: {e}")
#     finally:
#         cursor.close()
#         conn.close()