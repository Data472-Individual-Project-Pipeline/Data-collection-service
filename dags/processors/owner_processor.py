import os
import glob
import logging
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook

class OwnerManager:
    def __init__(self, postgres_conn_id, dags_folder):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.dags_folder = dags_folder

    def name_info_dict(self):
        name_info = """
                    [
                        {
                            "name": "hwa205",
                            "fullname": "Hua Wang",
                            "email": "hwa205@uclive.ac.nz"
                        },
                        {
                            "name": "tya51",
                            "fullname": "Tao Yan",
                            "email": "tya51@uclive.ac.nz"
                        },
                        {
                            "name": "jjm148",
                            "fullname": "Julian Maranan & Sridhar Vannada",
                            "email": "jjm148@uclive.ac.nz"
                        },
                        {
                            "name": "dus15",
                            "fullname": "Dimitrii Ustinov",
                            "email": "dus15@uclive.ac.nz"
                        },
                        {
                            "name": "rna104",
                            "fullname": "Roman Naumov",
                            "email": "rna104@uclive.ac.nz"
                        },
                        {
                            "name": "hpa117",
                            "fullname": "Haritha Parthiban",
                            "email": "hpa117@uclive.ac.nz"
                        },
                        {
                            "name": "ruben",
                            "fullname": "Ruben Castaing",
                            "email": "ruben.castaing@pg.canterbury.ac.nz"
                        },
                        {
                            "name": "sss135",
                            "fullname": "Shahron Shaji ~",
                            "email": "sss135@uclive.ac.nz"
                        }
                    ]
                    """
        return json.loads(name_info)

    def get_owner_names(self):
        logging.info("Scanning DAGs folder for Python files...")
        # Scan the dags folder for DAG files, excluding subfolders and specific files
        dag_files = glob.glob(os.path.join(self.dags_folder, "*.py"))
        exclude_files = {
            "owners_create.py",
            "owners_update.py",
            "owners.py",
            "dag_collection_data_from_bp.py",
            "dag_collection_data_from_mobil.py",
            "dag_collection_data_from_paknsave.py",
            "dag_collection_data_from_z.py",
            "dag_create_gas_station_table.py",
            "dag_daily_fuel_price_generation.py",
        }
        
        logging.info(f"Found DAG files: {dag_files}")
        logging.info(f"Excluding files: {exclude_files}")
        
        owner_names = [
            os.path.splitext(os.path.basename(f))[0]
            for f in dag_files
            if os.path.isfile(f) and os.path.basename(f) not in exclude_files
        ]
        
        logging.info(f"Detected owner names: {owner_names}")
        return owner_names

    def check_and_create_table(self):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS owners (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE,
                    fullname TEXT,
                    email TEXT,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
            conn.commit()
        except Exception as e:
            logging.error(f"Error creating table: {e}")
        finally:
            cursor.close()
            conn.close()

    def insert_owners(self, owner_names):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        name_info_list = self.name_info_dict()
        
        # 打印 owner_names 和 name_info_list 进行调试
        logging.info(f"Owner names: {owner_names}")
        logging.info(f"Name info list: {name_info_list}")
        
        try:
            for name in owner_names:
                owner_info = next((info for info in name_info_list if info['name'] == name), None)
                if owner_info:
                    fullname = owner_info['fullname']
                    email = owner_info['email']
                    cursor.execute(
                        """
                        INSERT INTO owners (name, fullname, email) VALUES (%s, %s, %s)
                        ON CONFLICT (name) DO NOTHING
                    """,
                        (name, fullname, email),
                    )
                    logging.info(f"Inserted owner: {name} - {fullname} - {email}")
                else:
                    logging.warning(f"No matching info found for {name}")
            conn.commit()
        except Exception as e:
            logging.error(f"Error inserting owners: {e}")
        finally:
            cursor.close()
            conn.close()

    def fetch_owners(self):
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT name FROM owners")
            owners = cursor.fetchall()
            return [owner[0] for owner in owners]
        except Exception as e:
            logging.error(f"Error fetching owners: {e}")
        finally:
            cursor.close()
            conn.close()
