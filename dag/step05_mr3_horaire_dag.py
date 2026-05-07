# Exécuté par : airflow-scheduler / airflow-webserver

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator

NAMENODE = "namenode"
HADOOP_JAR = "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar"
OUTPUT_PATH = "/data-lake/processed/horaire"
INPUT_GLOB = "/data-lake/raw/velo_lyon/*/*.json"
WORKSPACE = "/workspace"  # racine du projet montée en read-only dans le conteneur namenode
MAPPER = "step05_mr3_mapper_horaire.py"
REDUCER = "step05_mr3_reducer_horaire.py"
# fichier GeoJSON des arrondissements distribué aux nodemanagers via -files,
# il sera disponible dans le cwd des tâches sous le nom de base seul
GEOJSON = "data/arrondissements-lyon.geojson"

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="05_mr3_horaire",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(hours=1),  # cohérent avec MR1/MR2
    catchup=False,
    is_paused_upon_creation=True,
    tags=["velo", "mapreduce", "mr3"],
) as dag:

    # Hadoop refuse d'écrire dans un répertoire existant.
    # -f rend la commande idempotente : pas d'erreur si le répertoire n'existe pas (premier run).
    nettoyer_sortie = BashOperator(
        task_id="nettoyer_sortie",
        bash_command=f"docker exec {NAMENODE} hdfs dfs -rm -r -f {OUTPUT_PATH}",
    )

    # Soumission du job au cluster YARN via le client Hadoop installé dans le conteneur namenode.
    # Voir step03_mr1_load_factor_dag.py pour le détail de l'invocation via docker exec.
    lancer_mr3 = BashOperator(
        task_id="lancer_mr3",
        bash_command=dedent(f"""
            docker exec {NAMENODE} bash -c '
              hadoop jar {HADOOP_JAR} \\
                -files {WORKSPACE}/{MAPPER},{WORKSPACE}/{REDUCER},{WORKSPACE}/{GEOJSON} \\
                -input "{INPUT_GLOB}" \\
                -output {OUTPUT_PATH} \\
                -mapper "python3 {MAPPER}" \\
                -reducer "python3 {REDUCER}"
            '
        """),
    )

    nettoyer_sortie >> lancer_mr3
