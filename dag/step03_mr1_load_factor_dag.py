# Exécuté par : airflow-scheduler / airflow-webserver

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator

NAMENODE = "namenode"
HADOOP_JAR = "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar"
OUTPUT_PATH = "/data-lake/processed/load_metrics"
INPUT_GLOB = "/data-lake/raw/velo_lyon/*/*.json"
WORKSPACE = "/workspace"  # racine du projet montée en read-only dans le conteneur namenode
MAPPER = "step03_mr1_mapper_load_factor.py"
REDUCER = "step03_mr1_reducer_load_factor.py"

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="03_mr1_load_factor",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(hours=1),  # une agrégation par heure suffit pour ce TP
    catchup=False,
    is_paused_upon_creation=True,
    tags=["velo", "mapreduce", "mr1"],
) as dag:

    # Hadoop refuse d'écrire dans un répertoire existant.
    # -f rend la commande idempotente : pas d'erreur si le répertoire n'existe pas (premier run).
    nettoyer_sortie = BashOperator(
        task_id="nettoyer_sortie",
        bash_command=f"docker exec {NAMENODE} hdfs dfs -rm -r -f {OUTPUT_PATH}",
    )

    # Soumission du job au cluster YARN via le client Hadoop installé dans le conteneur namenode.
    # `docker exec namenode` exécute la commande dans ce conteneur grâce au socket Docker
    # monté dans Airflow (cf. compose.yml).
    # Les fichiers Python sont distribués aux nodemanagers via -files (cache distribué).
    # Le single quote externe encapsule le bash -c pour préserver les double quotes internes.
    lancer_mr1 = BashOperator(
        task_id="lancer_mr1",
        bash_command=dedent(f"""
            docker exec {NAMENODE} bash -c '
              hadoop jar {HADOOP_JAR} \\
                -files {WORKSPACE}/{MAPPER},{WORKSPACE}/{REDUCER} \\
                -input "{INPUT_GLOB}" \\
                -output {OUTPUT_PATH} \\
                -mapper "python3 {MAPPER}" \\
                -reducer "python3 {REDUCER}"
            '
        """),
    )

    nettoyer_sortie >> lancer_mr1
