# Data Collection Service

## Project Overview

The Data Collection Service is a project that DATA472 Central Collection Team used to collect other student individual project data, based on Apache Airflow designed to automate the collection and processing of student data. The project includes multiple DAGs (Directed Acyclic Graphs) and processors that collect data from different sources and store it in a database.

## Directory Structure

```bash
Data-collection-service/
│
├── config/
│ └── config.py # Configuration settings
│
├── dags/
│ ├── are154.py # Individual DAG files
│ ├── dus15.py
│ ├── hpa117.py
│ ├── hra80.py
│ ├── hwa205.py
│ ├── jjm148.py
│ ├── owners_create.py
│ ├── owners_update.py
│ ├── pvv13.py
│ ├── rna104.py
│ ├── ruben.py
│ ├── sss135.py
│ ├── svi40.py
│ ├── tya51.py
│
├── processors/
│ ├── init.py # Initialize processors package
│ ├── are154_processor.py # Individual processor files
│ ├── dus15_processor.py
│ ├── hpa117_processor.py
│ ├── hra80_processor.py
│ ├── hwa205_processor.py
│ ├── jjm148_processor.py
│ ├── owner_processor.py
│ ├── pvv13_processor.py
│ ├── rna104_processor.py
│ ├── ruben_processor.py
│ ├── prisoner_processor.py
│ ├── tya51_processor.py
│
├── webserver_config.py # Web server configuration
├── airflow.cfg # Airflow configuration file
└── README.md # Project documentation
```

## Installation and Setup

1. Clone the project repository:

    ```sh
    git clone https://github.com/yourusername/Data-collection-service.git
    cd Data-collection-service
    ```

2. Install dependencies:

   > I highly recommend using a virtual environment to install the dependencies

    ```sh
    pip install -r requirements.txt
    ```

3. Configure Airflow:

    - Edit the `airflow.cfg` file and adjust settings as needed.
    - Update the `config/config.py` file with appropriate configuration values.

4. Initialize the Airflow database:

    ```sh
    airflow db init
    ```

5. Create an Airflow user:

    ```sh
    airflow users create \
        --username admin \
        --firstname FIRST_NAME \
        --lastname LAST_NAME \
        --role Admin \
        --email admin@example.com
    ```

## Alternative Installation

1. Use Docker to run the Airflow web server and scheduler:

> You can download the docker compose file from the airflow official repository [here](https://github.com/apache/airflow)

```sh
docker-compose up
```

## Usage

1. Start the Airflow web server:

    ```sh
    airflow webserver --port 8080
    ```

2. Start the Airflow scheduler:

    ```sh
    airflow scheduler
    ```

3. Access the Airflow UI in your browser at [http://localhost:8080](http://localhost:8080).

4. Configure and enable the desired DAGs:

    - Log in to the Airflow UI.
    - Enable the required DAGs and monitor their execution.

## Example

Here is an example DAG file `jjm148.py`:

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from processors.jjm148_processor import process_jjm148

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'jjm148',
    default_args=default_args,
    description='JJM148 data collection DAG',
    schedule_interval='@daily',
)

t1 = PythonOperator(
    task_id='process_jjm148',
    python_callable=process_jjm148,
    dag=dag,
)
```

## Contributing

We welcome contributions! Please read the following instructions to get started:

1. Fork the project.
2. Create a new branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -am 'Add new feature`).
4. Push to the branch (`git push origin feature-branch`).
5. Create a new Pull Request.
