import json
import os
from datetime import datetime, timezone

import requests
from confluent_kafka import Consumer, KafkaError

TOPIC = "velo_lyon_raw"
HDFS_BASE = "/data-lake/raw/velo_lyon"


def consume_and_write_hdfs(
    broker: str | None = None,
    namenode_url: str | None = None,
) -> int | None:
    """Consomme les messages Kafka disponibles et écrit le dernier snapshot dans HDFS.
    Retourne le nombre de stations écrites, ou None si aucun message à traiter."""
    # paramètres optionnels : si non fournis, lecture depuis l'environnement
    broker = broker or os.environ.get("KAFKA_BROKER", "kafka:9092")
    namenode_url = namenode_url or os.environ.get("NAMENODE_URL", "http://namenode:9870")

    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": "airflow-hdfs-writer",  # groupe dédié pour suivre les offsets indépendamment du sensor
        "auto.offset.reset": "earliest",    # reprendre depuis le début si aucun offset commité
        "enable.auto.commit": False,         # commit manuel pour garantir l'écriture HDFS avant d'avancer
    })
    consumer.subscribe([TOPIC])

    messages = []
    empty_polls = 0
    # tolère plusieurs polls vides au démarrage : la première poll() après subscribe()
    # déclenche le rebalance des partitions et peut retourner None avant assignation
    MAX_EMPTY_POLLS = 3

    while empty_polls < MAX_EMPTY_POLLS:
        msg = consumer.poll(timeout=3.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break  # fin de partition atteinte, pas une vraie erreur
            raise Exception(f"Erreur Kafka : {msg.error()}")
        empty_polls = 0  # reset le compteur dès qu'un message arrive
        messages.append(json.loads(msg.value().decode("utf-8")))
        consumer.commit(asynchronous=False)  # commit synchrone : garantit la prise en compte avant de continuer

    consumer.close()

    if not messages:
        return None

    now = datetime.now(timezone.utc)
    hdfs_dir = f"{HDFS_BASE}/{now.strftime('%Y-%m-%d-%H')}"
    hdfs_path = f"{hdfs_dir}/stations_{now.strftime('%H%M%S')}.json"

    # créer le répertoire de destination s'il n'existe pas encore
    requests.put(
        f"{namenode_url}/webhdfs/v1{hdfs_dir}?op=MKDIRS&user.name=root"
    ).raise_for_status()

    # WebHDFS fonctionne en deux étapes : le namenode répond avec un redirect (307)
    # vers le datanode qui stocke effectivement le bloc
    payload = json.dumps(messages[-1]).encode("utf-8")  # on écrit uniquement le dernier snapshot
    r = requests.put(
        f"{namenode_url}/webhdfs/v1{hdfs_path}?op=CREATE&user.name=root&overwrite=true",
        allow_redirects=False,  # ne pas suivre automatiquement : on doit envoyer les données à la seconde URL
    )
    requests.put(r.headers["Location"], data=payload).raise_for_status()

    return len(messages[-1])


def main():
    """Exécution manuelle ponctuelle (test) : un cycle consume + write.
    La périodicité est gérée par le DAG Airflow pipeline_velo_lyon."""
    try:
        n = consume_and_write_hdfs()
        if n is None:
            print("Aucun message à traiter.")
        else:
            print(f"[OK] {n} stations écrites dans HDFS.")
    except Exception as e:
        print(f"[ERREUR] {e}")


if __name__ == "__main__":
    main()
