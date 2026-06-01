#!/bin/bash
# Exécuté par : dev (post-reset_data_lake.sh)
# Redémarre proprement les DAGs Airflow après un reset du data lake.
#
# Pourquoi : dépauser tous les DAGs simultanément après reset crée des runs
# "scheduled" immédiats pour les MRs (cadence horaire/journalière) qui partent
# avant que le pipeline 02 ait écrit le premier snapshot dans HDFS. Résultat :
# "Input Pattern matches 0 files" → exit 5 → runs failed.
#
# Stratégie : on dépause par vagues, avec une attente sur la présence effective
# d'un .json dans HDFS entre amont (01,02) et MRs (03,04,05,06).

set -euo pipefail

PG_OPTS="-h postgres-airflow -U airflow -d airflow"
NAMENODE="${NAMENODE_URL:-http://namenode:9870}"
RAW_GLOB_PATH="/data-lake/raw/velo_lyon"
TIMEOUT_SECONDS=420  # 7 min : 1 run du producer (3 min) + 1 run du pipeline (3 min) + marge

echo "=== Redémarrage du pipeline ==="

echo
echo "[1/3] Dépausage de 01_kafka_producer_velo et 02_pipeline_velo_lyon"
PGPASSWORD=airflow psql $PG_OPTS -v ON_ERROR_STOP=1 -c \
  "UPDATE dag SET is_paused = false WHERE dag_id IN ('01_kafka_producer_velo', '02_pipeline_velo_lyon');"

echo
echo "[2/3] Attente d'au moins 1 snapshot dans HDFS (timeout: ${TIMEOUT_SECONDS}s)"
# Polling de WebHDFS : on liste les sous-répertoires horaires de raw/velo_lyon
# et on regarde s'il existe au moins un fichier .json dedans.
elapsed=0
while [ $elapsed -lt $TIMEOUT_SECONDS ]; do
  # liste les sous-répertoires (YYYY-MM-DD-HH)
  subdirs=$(curl -s "${NAMENODE}/webhdfs/v1${RAW_GLOB_PATH}?op=LISTSTATUS&user.name=root" \
    | python -c "import sys,json; d=json.load(sys.stdin); print('\n'.join(f['pathSuffix'] for f in d['FileStatuses']['FileStatus']))" 2>/dev/null || true)
  for sd in $subdirs; do
    nb=$(curl -s "${NAMENODE}/webhdfs/v1${RAW_GLOB_PATH}/${sd}?op=LISTSTATUS&user.name=root" \
      | python -c "import sys,json; d=json.load(sys.stdin); print(sum(1 for f in d['FileStatuses']['FileStatus'] if f['pathSuffix'].endswith('.json')))" 2>/dev/null || echo 0)
    if [ "$nb" -gt 0 ]; then
      echo "  - $nb fichier(s) .json détecté(s) dans ${RAW_GLOB_PATH}/${sd}"
      break 2
    fi
  done
  echo "  - aucun .json encore, attente 15s (écoulé: ${elapsed}s)"
  sleep 15
  elapsed=$((elapsed + 15))
done

if [ $elapsed -ge $TIMEOUT_SECONDS ]; then
  echo "ERREUR : timeout — aucune donnée dans HDFS après ${TIMEOUT_SECONDS}s. Vérifier 01 et 02."
  exit 1
fi

echo
echo "[3/3] Dépausage des MRs (03, 04, 05, 06)"
PGPASSWORD=airflow psql $PG_OPTS -v ON_ERROR_STOP=1 -c \
  "UPDATE dag SET is_paused = false WHERE dag_id IN ('03_mr1_load_factor', '04_mr2_anomalies', '05_mr3_horaire', '06_mr4_heatmap');"

echo
echo "=== Terminé ==="
echo "Surveiller l'UI Airflow (Ctrl+Shift+R). Les MRs vont lancer leur premier run dans la minute."
