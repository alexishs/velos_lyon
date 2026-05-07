# Exécuté par : airflow-scheduler / airflow-webserver

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# le module kafka_producer est mis à disposition d'Airflow via le mount /opt/airflow/lib
# référencé dans PYTHONPATH (cf. compose.yml)
from step01_kafka_producer import fetch_and_publish, TOPIC


def publier():
    n = fetch_and_publish()  # lit JCDECAUX_API_KEY et KAFKA_BROKER depuis l'environnement Airflow
    print(f"[OK] {n} stations publiées dans '{TOPIC}'")


default_args = {
    "owner": "airflow",
    "retries": 2,                       # 2 tentatives en cas d'échec (API JCDecaux peut être temporairement indisponible)
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="01_kafka_producer_velo",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(minutes=3),  # même cadence que la version standalone
    catchup=False,                           # pas de rattrapage des exécutions passées
    is_paused_upon_creation=True,            # DAG en pause à sa création, à activer manuellement dans l'UI
    tags=["velo", "kafka", "producer"],
) as dag:

    fetch_et_publier = PythonOperator(
        task_id="fetch_et_publier",
        python_callable=publier,
    )
