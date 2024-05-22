import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

class Sss135Processor:
    def __init__(self, postgres_conn_id, api_url):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.api_url = api_url
        self.logger = logging.getLogger("sss135_processor")

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
            self.logger.info("Checking and creating tables for sss135 data")
            tables = [
                {
                    "name": "sss135_age_group",
                    "create_statement": """
                        CREATE TABLE IF NOT EXISTS sss135_age_group (
                            id SERIAL PRIMARY KEY,
                            age_group TEXT,
                            date DATE,
                            observations REAL,
                            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            owner TEXT
                        )
                    """
                },
                {
                    "name": "sss135_ethnicity",
                    "create_statement": """
                        CREATE TABLE IF NOT EXISTS sss135_ethnicity (
                            id SERIAL PRIMARY KEY,
                            date DATE,
                            ethnicity TEXT,
                            observations REAL,
                            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            owner TEXT
                        )
                    """
                },
                {
                    "name": "sss135_offence_type",
                    "create_statement": """
                        CREATE TABLE IF NOT EXISTS sss135_offence_type (
                            id SERIAL PRIMARY KEY,
                            date DATE,
                            offence_type TEXT,
                            observations REAL,
                            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            owner TEXT
                        )
                    """
                },
                {
                    "name": "sss135_prisoner_population",
                    "create_statement": """
                        CREATE TABLE IF NOT EXISTS sss135_prisoner_population (
                            id SERIAL PRIMARY KEY,
                            date DATE,
                            gender TEXT,
                            location TEXT,
                            observations REAL,
                            population_type TEXT,
                            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            owner TEXT
                        )
                    """
                },
                {
                    "name": "sss135_security_class",
                    "create_statement": """
                        CREATE TABLE IF NOT EXISTS sss135_security_class (
                            id SERIAL PRIMARY KEY,
                            date DATE,
                            security_class TEXT,
                            observations REAL,
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

    def insert_items(self, items, owner):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            for category, data in items.items():
                count = 0
                duplicate_count = 0
                if category == "Age Group":
                    for item in data:
                        cursor.execute("""
                            SELECT 1 FROM sss135_age_group WHERE 
                                age_group = %s AND 
                                date = %s AND 
                                observations = %s
                        """, (
                            item['Age group'],
                            item['Date'],
                            item['Observations']
                        ))

                        if cursor.fetchone():
                            duplicate_count += 1
                        else:
                            cursor.execute("""
                                INSERT INTO sss135_age_group (
                                    age_group, date, observations, owner
                                ) VALUES (%s, %s, %s, %s)
                            """, (
                                item['Age group'],
                                item['Date'],
                                item['Observations'],
                                owner
                            ))
                            count += 1
                elif category == "Ethnicity":
                    for item in data:
                        cursor.execute("""
                            SELECT 1 FROM sss135_ethnicity WHERE 
                                ethnicity = %s AND 
                                date = %s AND 
                                observations = %s
                        """, (
                            item['Ethnicity'],
                            item['Date'],
                            item['Observations']
                        ))

                        if cursor.fetchone():
                            duplicate_count += 1
                        else:
                            cursor.execute("""
                                INSERT INTO sss135_ethnicity (
                                    ethnicity, date, observations, owner
                                ) VALUES (%s, %s, %s, %s)
                            """, (
                                item['Ethnicity'],
                                item['Date'],
                                item['Observations'],
                                owner
                            ))
                            count += 1
                elif category == "Offence Type":
                    for item in data:
                        cursor.execute("""
                            SELECT 1 FROM sss135_offence_type WHERE 
                                offence_type = %s AND 
                                date = %s AND 
                                observations = %s
                        """, (
                            item['Offense type'],
                            item['Date'],
                            item['Observations']
                        ))

                        if cursor.fetchone():
                            duplicate_count += 1
                        else:
                            cursor.execute("""
                                INSERT INTO sss135_offence_type (
                                    offence_type, date, observations, owner
                                ) VALUES (%s, %s, %s, %s)
                            """, (
                                item['Offense type'],
                                item['Date'],
                                item['Observations'],
                                owner
                            ))
                            count += 1
                elif category == "Prisoner Population":
                    for item in data:
                        cursor.execute("""
                            SELECT 1 FROM sss135_prisoner_population WHERE 
                                gender = %s AND 
                                location = %s AND 
                                date = %s AND 
                                population_type = %s
                        """, (
                            item['Gender'],
                            item['Location'],
                            item['Date'],
                            item['Population Type']
                        ))

                        if cursor.fetchone():
                            duplicate_count += 1
                        else:
                            cursor.execute("""
                                INSERT INTO sss135_prisoner_population (
                                    gender, location, date, observations, population_type, owner
                                ) VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                item['Gender'],
                                item['Location'],
                                item['Date'],
                                item['Observations'],
                                item['Population Type'],
                                owner
                            ))
                            count += 1
                elif category == "Security Class":
                    for item in data:
                        cursor.execute("""
                            SELECT 1 FROM sss135_security_class WHERE 
                                security_class = %s AND 
                                date = %s AND 
                                observations = %s
                        """, (
                            item['Security class'],
                            item['Date'],
                            item['Observations']
                        ))

                        if cursor.fetchone():
                            duplicate_count += 1
                        else:
                            cursor.execute("""
                                INSERT INTO sss135_security_class (
                                    security_class, date, observations, owner
                                ) VALUES (%s, %s, %s, %s)
                            """, (
                                item['Security class'],
                                item['Date'],
                                item['Observations'],
                                owner
                            ))
                            count += 1
                conn.commit()
                self.logger.info(f"{count} items inserted into {category.lower().replace(' ', '_')}.")
                self.logger.info(f"{duplicate_count} items were duplicates and not inserted into {category.lower().replace(' ', '_')}.")
        except Exception as e:
            self.logger.error(f"Error inserting items: {e}")
        finally:
            cursor.close()
            conn.close()
