import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

class Tya51DataProcessor:
    def __init__(self, postgres_conn_id):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    def fetch_data(self, url):
        logging.info(f"Fetching data from {url}")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Fetched data: {data}")
        return data

    def check_and_create_table(self, table_name, schema):
        conn = self.hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        );
        """)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            logging.info(f"Creating table {table_name}")
            cursor.execute(schema)
            conn.commit()
        
        cursor.close()
        conn.close()

    def insert_data(self, table_name, data_list, owner):
        conn = self.hook.get_conn()
        cursor = conn.cursor()

        for data in data_list:
            data['owner'] = owner
            data['inserted_at'] = datetime.utcnow().isoformat()

            columns = ', '.join(data.keys())
            values = ', '.join([f'%({k})s' for k in data.keys()])
            update_values = ', '.join([f"{k} = EXCLUDED.{k}" for k in data.keys() if k != 'locationId'])

            logging.info(f"Inserting data into {table_name}: {data}")
            cursor.execute(f"""
            INSERT INTO {table_name} ({columns}) VALUES ({values})
            ON CONFLICT (locationId)
            DO UPDATE SET {update_values};
            """, data)
            conn.commit()
        
        cursor.close()
        conn.close()

def try_fetch_data_for_days(processor, base_url, days=5):
    for i in range(days):
        date_str = (datetime.utcnow() - timedelta(days=i)).strftime('%Y%m%d')
        url = f"{base_url}/{date_str}"
        data = processor.fetch_data(url)
        if data:
            logging.info(f"Data found for date {date_str}")
            return data
        logging.info(f"No data found for date {date_str}, trying previous day")
    logging.warning("No data found for the past 5 days")
    return []