import logging
from datetime import datetime

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook


class Hra80Processor:
    def __init__(self, postgres_conn_id):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def fetch_data(self, url):
        logging.info(f"Fetching data from {url}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            logging.info(f"Fetched data: {data}")
            return data
        except requests.exceptions.HTTPError as err:
            if response.status_code == 404:
                logging.warning(f"Data not found for {url}, status code: 404")
                return []
            else:
                raise err

    def insert_data(self, table_name, data_list, owner):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        for data in data_list:
            # Rename observationtime to timestamp
            if 'ObservationTime' in data:
                data['timestamp'] = data.pop('ObservationTime')
            data['owner'] = owner
            data['inserted_at'] = datetime.utcnow().isoformat()
            columns = ', '.join(data.keys())
            values = ', '.join([f'%({k})s' for k in data.keys()])
            if table_name == 'riverflow_observation':
                update_values = ', '.join(
                    [f"{k} = EXCLUDED.{k}" for k in data.keys() if k not in ('locationId', 'timestamp')])
                conflict_columns = '(locationId, timestamp)'
            else:
                update_values = ', '.join([f"{k} = EXCLUDED.{k}" for k in data.keys() if k != 'locationId'])
                conflict_columns = '(locationId)'
            logging.info(f"Inserting data into {table_name}: {data}")
            cursor.execute(f"""
            INSERT INTO {table_name} ({columns}) VALUES ({values})
            ON CONFLICT {conflict_columns}
            DO UPDATE SET {update_values};
            """, data)
            conn.commit()
        cursor.close()
        conn.close()

    def process(self, location_url, observation_url, owner):
        # Fetch and insert location data
        location_data = self.fetch_data(location_url)
        self.insert_data('riverflow_location', location_data, owner)

        # Fetch and insert observation data
        observation_data = self.fetch_data(observation_url)
        if observation_data:
            self.insert_data('riverflow_observation', observation_data, owner)
        else:
            logging.warning("No observation data to insert")
