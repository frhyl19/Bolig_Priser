FROM apache/airflow:2.8.0

RUN pip install --no-cache-dir \
    "dbt-postgres==1.7.0" \
    "azure-storage-blob" \
    "Flask==2.2.5" \
    "Werkzeug==2.2.3"
