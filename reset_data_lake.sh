#!/bin/bash
# Exécuté par : dev (test manuel)
# Réinitialise le data lake : supprime les répertoires HDFS et reset le topic Kafka.

set -euo pipefail

NAMENODE="${NAMENODE_URL:-http://namenode:9870}"
KAFKA_BROKER="${KAFKA_BROKER:-kafka:29092}"
TOPIC="velo_lyon_raw"
DIRS=(/data-lake/raw /data-lake/processed /data-lake/analytics)

echo "=== Réinitialisation du data lake ==="

echo
echo "[1/5] Suppression des répertoires HDFS"
for dir in "${DIRS[@]}"; do
  echo "  - $dir"
  # WebHDFS DELETE récursif (silencieux si déjà absent)
  curl -s -X DELETE "${NAMENODE}/webhdfs/v1${dir}?op=DELETE&recursive=true&user.name=root" > /dev/null
done

echo
echo "[2/5] Recréation des répertoires HDFS"
for dir in "${DIRS[@]}"; do
  echo "  - $dir"
  curl -s -X PUT "${NAMENODE}/webhdfs/v1${dir}?op=MKDIRS&user.name=root" > /dev/null
done

echo
echo "[3/5] Reset du topic Kafka '$TOPIC'"
# Embedded Python : confluent-kafka est installé dans le conteneur dev
# (l'AdminClient remplace l'outil kafka-topics qui n'est dispo que sur le conteneur kafka)
python - <<PYEOF
import time
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "$KAFKA_BROKER"})

# suppression du topic (silencieux si déjà absent)
for t, f in admin.delete_topics(["$TOPIC"]).items():
    try:
        f.result()
        print(f"  - topic {t} supprimé")
    except Exception as e:
        print(f"  - {t} : {e}")

# pause pour laisser le cluster propager la suppression avant la recréation
time.sleep(3)

# recréation à l'identique de la création initiale (1 partition, replication 1)
for t, f in admin.create_topics([NewTopic("$TOPIC", num_partitions=1, replication_factor=1)]).items():
    try:
        f.result()
        print(f"  - topic {t} créé")
    except Exception as e:
        print(f"  - {t} : {e}")
PYEOF

echo
echo "[4/5] Purge de l'historique Airflow en base"
# truncate des tables postgres-airflow contenant l'historique des runs et tâches.
# Les tables de config (utilisateurs, connexions, variables) sont préservées.
# CASCADE propage la troncation aux tables liées par foreign key.
PGPASSWORD=airflow psql -h postgres-airflow -U airflow -d airflow -v ON_ERROR_STOP=1 <<SQL
TRUNCATE TABLE
    dag_run,                        -- historique des exécutions de DAGs
    task_instance,                  -- historique de chaque tâche
    task_fail,                      -- échecs de tâches
    task_reschedule,                -- tâches mises en attente
    xcom,                           -- données XCom échangées entre tâches
    rendered_task_instance_fields,  -- rendu Jinja des paramètres de tâches
    log,                            -- log d'événements Airflow
    job,                            -- jobs scheduler/worker
    import_error,                   -- erreurs de parsing des DAGs
    sla_miss                        -- SLA non respectés
CASCADE;
SQL

echo
echo "[5/5] Suppression des logs Airflow"
# le volume airflow-logs est monté dans le conteneur dev (cf. compose.yml)
# sudo nécessaire car les fichiers de logs sont créés par le process Airflow
# avec un GID root, non accessible en écriture au user dev
if [ -d /airflow-logs ]; then
  sudo find /airflow-logs -mindepth 1 -delete
  echo "  - logs Airflow purgés"
else
  echo "  - /airflow-logs non monté, étape ignorée"
fi

echo
echo "=== Terminé ==="
