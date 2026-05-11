from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import requests
import pandas as pd
import io
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
DBT_DIR = "/opt/airflow/dbt"
AZURE_CONTAINER = "boligpriser-raw"


def fetch_bolig_data():
    meta = requests.post(
        "https://api.statbank.dk/v1/tableinfo",
        json={"table": "EJEN11", "lang": "da"},
        timeout=30,
    ).json()

    variables = [
        {"code": var["id"], "values": [v["id"] for v in var["values"]]}
        for var in meta["variables"]
    ]

    response = requests.post(
        "https://api.statbank.dk/v1/data",
        json={"table": "EJEN11", "format": "CSV", "lang": "da", "variables": variables},
        timeout=60,
    )

    if response.status_code != 200:
        raise ValueError(f"Statbank API fejlede: {response.status_code} — {response.text[:200]}")

    df = pd.read_csv(io.StringIO(response.text), sep=";")
    print(f"Antal rækker: {len(df)}")
    print(df.head())

    # Upload raw CSV to Azure Blob Storage
    conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_client = BlobServiceClient.from_connection_string(conn_str)
    container = blob_client.get_container_client(AZURE_CONTAINER)
    try:
        container.create_container()
    except ResourceExistsError:
        pass
    blob_name = f"ejen11_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    container.upload_blob(blob_name, response.text, overwrite=True)
    print(f"Uploadet til Azure Blob: {AZURE_CONTAINER}/{blob_name}")

    # Load into PostgreSQL
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS raw_boligpriser"))
    df.to_sql("raw_boligpriser", engine, if_exists="fail", index=False)
    print("Data indlæst i PostgreSQL: raw_boligpriser")

    return f"Hentet {len(df)} rækker fra EJEN11"


with DAG(
    dag_id="statbank_boligpriser",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_boligpriser",
        python_callable=fetch_bolig_data,
    )

    dbt_bronze = BashOperator(
        task_id="dbt_bronze",
        bash_command=f"dbt run --select bronze --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command=f"dbt run --select silver --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command=f"dbt run --select gold --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    fetch_task >> dbt_bronze >> dbt_silver >> dbt_gold
