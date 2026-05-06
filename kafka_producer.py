import json
import os

import requests
from confluent_kafka import Producer

API_URL = "https://api.jcdecaux.com/vls/v1/stations"
TOPIC = "velo_lyon_raw"


def fetch_stations(api_key: str) -> list:
    """Récupère l'état temps réel des stations Vélo'v depuis l'API JCDecaux."""
    response = requests.get(API_URL, params={"contract": "lyon", "apiKey": api_key})
    response.raise_for_status()  # lève une exception si l'API retourne une erreur HTTP
    return response.json()


def fetch_and_publish(api_key: str | None = None, broker: str | None = None) -> int:
    """Exécute un cycle complet : fetch API → publish Kafka. Retourne le nombre de stations publiées."""
    # paramètres optionnels : si non fournis, lecture depuis l'environnement
    api_key = api_key or os.environ["JCDECAUX_API_KEY"]
    broker = broker or os.environ.get("KAFKA_BROKER", "kafka:9092")

    producer = Producer({"bootstrap.servers": broker})

    stations = fetch_stations(api_key)
    payload = json.dumps(stations).encode("utf-8")
    # produce() est non-bloquant : le message est mis en file d'attente interne
    producer.produce(TOPIC, value=payload)
    # flush() attend que tous les messages en attente soient effectivement envoyés
    producer.flush()

    return len(stations)


def main():
    """Exécution manuelle ponctuelle (test) : un seul cycle fetch + publish.
    La périodicité est gérée par le DAG Airflow kafka_producer_velo."""
    try:
        n = fetch_and_publish()
        print(f"[OK] {n} stations publiées dans '{TOPIC}'")
    except Exception as e:
        print(f"[ERREUR] {e}")


if __name__ == "__main__":
    main()
