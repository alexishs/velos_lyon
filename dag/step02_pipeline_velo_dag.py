# Exécuté par : airflow-scheduler / airflow-webserver / airflow-triggerer (sensor deferrable)

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

# le module kafka_consumer est mis à disposition d'Airflow via le mount /opt/airflow/lib
# référencé dans PYTHONPATH (cf. compose.yml)
from step02_kafka_consumer import TOPIC, consume_and_write_hdfs


def tout_message(message):
    """Condition de déclenchement du sensor : accepte tout message non nul."""
    return message is not None


def ecrire_hdfs():
    n = consume_and_write_hdfs()  # lit KAFKA_BROKER et NAMENODE_URL depuis l'environnement Airflow
    if n is None:
        print("Aucun message à traiter.")
    else:
        print(f"[OK] {n} stations écrites dans HDFS.")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="02_pipeline_velo_lyon",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(minutes=3),
    catchup=False,                  # pas de rattrapage des exécutions passées
    is_paused_upon_creation=True,   # DAG en pause à sa création, à activer manuellement dans l'UI
    tags=["velo", "kafka", "hdfs"],
) as dag:

    attendre_donnees = AwaitMessageSensor(
        task_id="attendre_donnees_kafka",
        topics=[TOPIC],
        kafka_config_id="kafka_default",              # connexion définie via AIRFLOW_CONN_KAFKA_DEFAULT dans le compose
        apply_function="step02_pipeline_velo_dag.tout_message",  # chemin modulaire requis par le sensor (pas un callable direct)
        poll_interval=30,                             # intervalle entre deux sondes Kafka (secondes)
        execution_timeout=timedelta(minutes=2, seconds=30),  # abandonne avant le prochain cycle DAG
    )

    ecrire_hdfs_task = PythonOperator(
        task_id="ecrire_hdfs",
        python_callable=ecrire_hdfs,
    )

    # TODO: ajouter les tâches de déclenchement des jobs MapReduce MR1-MR4
    # TODO: ajouter les tâches de mise à jour des tables Hive

    attendre_donnees >> ecrire_hdfs_task
