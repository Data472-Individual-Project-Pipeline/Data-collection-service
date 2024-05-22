import os
import glob
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


class OwnerManager:
    def __init__(self, postgres_conn_id, dags_folder):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.dags_folder = dags_folder

    def get_owner_names(self):
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
        owner_names = [
            os.path.splitext(os.path.basename(f))[0]
            for f in dag_files
            if os.path.isfile(f) and os.path.basename(f) not in exclude_files
        ]
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
        try:
            for name in owner_names:
                cursor.execute(
                    """
                    INSERT INTO owners (name) VALUES (%s)
                    ON CONFLICT (name) DO NOTHING
                """,
                    (name,),
                )
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
