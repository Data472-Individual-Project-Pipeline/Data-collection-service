import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_and_check_tables(postgres_conn_id):
    logging.info("Creating and checking tables")
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    location_table_schema = """
    CREATE TABLE IF NOT EXISTS riverflow_location (
        locationId VARCHAR PRIMARY KEY,
        name VARCHAR,
        nztmx INTEGER,
        nztmy INTEGER,
        type VARCHAR,
        unit VARCHAR,
        owner VARCHAR,
        inserted_at TIMESTAMP
    );
    """
    observation_table_schema = """
    CREATE TABLE IF NOT EXISTS riverflow_observation (
        locationId VARCHAR,
        qualityCode VARCHAR,
        timestamp TIMESTAMP,
        value FLOAT,
        owner VARCHAR,
        inserted_at TIMESTAMP,
        PRIMARY KEY (locationId, timestamp)
    );
    """
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    for schema in [location_table_schema, observation_table_schema]:
        cursor.execute(schema)
    
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Tables created and checked successfully")
