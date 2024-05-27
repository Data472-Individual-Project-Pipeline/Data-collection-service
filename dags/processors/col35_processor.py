import logging

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook


class Col35Processor:
    def __init__(self, postgres_conn_id, api_url):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.api_url = api_url
        self.logger = logging.getLogger("col35_processor")

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
            self.logger.info("Checking and creating table prisoner_age_group")
            create_tables_query = """
                CREATE TABLE IF NOT EXISTS prisoner_age_group (
                    id SERIAL PRIMARY KEY,
                    age_group TEXT,
                    date DATE,
                    observations REAL,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT
                );
            
                CREATE TABLE IF NOT EXISTS prisoner_ethnicity (
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    ethnicity TEXT,
                    observations REAL,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT
                );
            
                CREATE TABLE IF NOT EXISTS prisoner_offence_type (
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    offence_type TEXT,
                    observations REAL,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT
                );
            
                CREATE TABLE IF NOT EXISTS prisoner_population (
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    gender TEXT,
                    location TEXT,
                    observations REAL,
                    population_type TEXT,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT
                );
            
                CREATE TABLE IF NOT EXISTS prisoner_number_sentences (
                    id SERIAL PRIMARY KEY,
                    city TEXT,
                    date DATE,
                    observations REAL,
                    type TEXT,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT
                );
            
                CREATE TABLE IF NOT EXISTS prisoner_sentence_type (
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    observations REAL,
                    type TEXT,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT
                );
            """

            # Execute the query
            cursor.execute(create_tables_query)
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error creating table: {e}")
        finally:
            cursor.close()
            conn.close()

    def insert_items(self, item_data, owner):
        # try a hack to see if it works
        self._insert_age_group(item_data['Age'], owner)
        self._insert_ethnicity(item_data['Ethnicity'], owner)
        self._insert_gender(item_data['Gender'], owner)
        self._insert_number_of_sentences(item_data['NoOfSentences'], owner)
        self._insert_offence_type(item_data['Offencetype'], owner)
        self._insert_sentence_type(item_data['Type'], owner)

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

    def _insert_age_group(self, items, owner):
        self.execute_insert_items(
            items,
            table='prisoner_age_group',
            unique_fields=['age_group', 'date', 'observations'],
            table_fields=['Age', 'Date', 'Observations'],
            owner=owner
        )

    def _insert_ethnicity(self, items, owner):
        self.execute_insert_items(
            items,
            table='prisoner_ethnicity',
            unique_fields=['date', 'ethnicity', 'observations'],
            table_fields=['Date', 'Ethnicity', 'Observations'],
            owner=owner
        )

    def _insert_gender(self, items, owner):
        self.execute_insert_items(
            items,
            table='prisoner_population',
            unique_fields=['date', 'gender', 'observations'],
            table_fields=['Date', 'Gender', 'Observations'],
            owner=owner
        )

    def _insert_number_of_sentences(self, items, owner):
        self.execute_insert_items(
            [item for item in items if item['Type'] != 'Total'],
            table='prisoner_number_sentences',
            unique_fields=['city', 'date', 'observations', 'type'],
            table_fields=['City', 'Date', 'Observations', 'Type'],
            owner=owner
        )

    def _insert_offence_type(self, items, owner):
        self.execute_insert_items(
            items,
            table='prisoner_offence_type',
            unique_fields=['date', 'observations', 'offence_type'],
            table_fields=['Date', 'Observations', 'Offence type'],
            owner=owner
        )

    def _insert_sentence_type(self, items, owner):
        self.execute_insert_items(
            items,
            table='prisoner_sentence_type',
            unique_fields=['date', 'observations', 'type'],
            table_fields=['Date', 'Observations', 'Type'],
            owner=owner
        )
