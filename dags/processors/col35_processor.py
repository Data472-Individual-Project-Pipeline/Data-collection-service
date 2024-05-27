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
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS prisoner_age_group (
                    id SERIAL PRIMARY KEY,
                    age_group TEXT,
                    date DATE,
                    observations FLOAT64,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT
                );
                CREATE TABLE IF NOT EXISTS prisoner_ethnicity (
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    ethnicity TEXT,
                    observations FLOAT64,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT
                );
                CREATE TABLE IF NOT EXISTS prisoner_offence_type (
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    offence_type TEXT,
                    observations FLOAT64,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT                    
                );
               CREATE TABLE IF NOT EXISTS prisoner_population (
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    gender TEXT,
                    location TEXT,
                    observations FLOAT64,
                    population_type TEXT,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT                                                  
               );
               CREATE TABLE IF NOT EXISTS prisoner_number_sentences (
                    id SERIAL PRIMARY KEY,
                    city TEXT,
                    date DATE,
                    observations FLOAT64,
                    type TEXT,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT                     
               );
                CREATE TABLE IF NOT EXISTS prisoner_sentence_type (
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    observations FLOAT64,
                    type TEXT,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT                     
               );
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
            # modify for this selection because each field is different.

            for item in items['Age']:
                cursor.execute("""
                    SELECT 1 FROM prisoner_age_group WHERE 
                        age_group = %s AND 
                        date = %s AND 
                        observations = %s
                """, (
                    item['Age'],
                    item['Date'],
                    item['Observations']
                ))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                        INSERT INTO prisoner_age_group (
                            age_group, date, observations, owner
                        ) VALUES (%s, %s, %s, %s)
                    """, (
                        item['Age'],
                        item['Date'],
                        item['Observations'],
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
        # second header
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            count = 0
            duplicate_count = 0
            # modify for this selection because each field is different.

            for item in items['Ethnicity']:
                cursor.execute("""
                    SELECT 1 FROM prisoner_ethnicity WHERE 
                        date = %s AND 
                        ethnicity = %s AND 
                        observations = %s
                """, (
                    item['Date'],
                    item['Ethnicity'],
                    item['Observations']
                ))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                        INSERT INTO prisoner_ethnicity (
                            date, ethnicity, observations, owner
                        ) VALUES (%s, %s, %s, %s)
                    """, (
                        item['Age'],
                        item['Date'],
                        item['Observations'],
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
        # third connector
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            count = 0
            duplicate_count = 0
            # modify for this selection because each field is different.

            for item in items['Gender']:
                cursor.execute("""
                    SELECT 1 FROM prisoner_population WHERE 
                        date = %s AND 
                        gender = %s AND 
                        observations = %s
                """, (
                    item['Date'],
                    item['Gender'],
                    item['Observations']
                ))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                        INSERT INTO prisoner_population (
                            date, gender, observations, owner
                        ) VALUES (%s, %s, %s, %s)
                    """, (
                        item['Date'],
                        item['Gender'],
                        item['Observations'],
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
        # NoOfSentences
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            count = 0
            duplicate_count = 0
            # modify for this selection because each field is different.
            for item in items['NoOfSentences']:
                if not item['Type'] == 'Total':
                    cursor.execute("""
                                    SELECT 1 FROM prisoner_number_sentences WHERE
                                        city = %s AND 
                                        date = %s AND 
                                        observations = %s AND 
                                        type = %s
                                """, (
                        item['City'],
                        item['Date'],
                        item['Observations'],
                        item['Type']

                    ))

                    if cursor.fetchone():
                        duplicate_count += 1
                    else:
                        cursor.execute("""
                                        INSERT INTO prisoner_number_sentences (
                                            city, date, observations, type, owner
                                        ) VALUES (%s, %s, %s, %s, %s)
                                    """, (
                            item['City'],
                            item['Date'],
                            item['Observations'],
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

        # OffenceType
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            count = 0
            duplicate_count = 0
            # modify for this selection because each field is different.

            for item in items['OffenceType']:
                cursor.execute("""
                        SELECT 1 FROM prisoner_offence_type WHERE 
                            date = %s AND 
                            observations = %s AND
                            offence_type = %s
                    """, (
                    item['Date'],
                    item['Observations'],
                    item['Offence type']
                ))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                            INSERT INTO prisoner_offence_type (
                                date, observations, offence_type, owner
                            ) VALUES (%s, %s, %s, %s)
                        """, (
                        item['Date'],
                        item['Observations'],
                        item['Offence type'],
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

        # Type (sentence_type)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            count = 0
            duplicate_count = 0
            # modify for this selection because each field is different.

            for item in items['Type']:
                cursor.execute("""
                        SELECT 1 FROM prisoner_sentence_type WHERE 
                            date = %s AND 
                            observations = %s AND
                            type = %s
                    """, (
                    item['Date'],
                    item['Observations'],
                    item['Type']
                ))

                if cursor.fetchone():
                    duplicate_count += 1
                else:
                    cursor.execute("""
                            INSERT INTO prisoner_offence_type (
                                date, observations, type, owner
                            ) VALUES (%s, %s, %s, %s)
                        """, (
                        item['Date'],
                        item['Observations'],
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