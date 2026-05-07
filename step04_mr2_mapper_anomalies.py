#!/usr/bin/env python3
# Exécuté par : nodemanager (Hadoop Streaming) / dev (test manuel)
"""MR2 mapper — détecte les anomalies par station depuis chaque snapshot JSONL.
Émet une ligne par station observée : type d'anomalie détectée ou 'OK'.

Schéma d'entrée : API JCDecaux v1 (number, last_update en ms, available_bikes,
available_bike_stands, bike_stands, status). Cohérent avec le reste du pipeline.
"""

import json
import sys
import time

# seuils définis par l'énoncé
NO_UPDATE_THRESHOLD_S = 30 * 60  # 30 minutes


def main():
    # référence temporelle utilisée pour calculer l'ancienneté des relevés.
    # On prend l'horloge système au démarrage du mapper (le job tourne "maintenant"),
    # ce qui est cohérent avec un usage de monitoring temps réel.
    now = int(time.time())

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            station = json.loads(line)
        except json.JSONDecodeError:
            continue  # contrainte énoncé : JSON malformé → ligne ignorée

        try:
            station_id = station["number"]
            available_bikes = station["available_bikes"]
            bike_stands = station["bike_stands"]
            status = station["status"]
            # last_update vient en millisecondes epoch (API v1) → conversion en secondes
            last_update = station["last_update"] // 1000
        except (KeyError, TypeError):
            continue  # champ manquant ou type invalide → ligne ignorée

        age_last_update = now - last_update

        # détection par priorité décroissante : NO_UPDATE > FULL_STANDS > ZERO_BIKES
        # une seule anomalie émise par observation pour ne pas double-compter
        anomaly_type = "OK"

        if age_last_update > NO_UPDATE_THRESHOLD_S:
            # données plus vieilles que 30 min → capteur muet ou borne déconnectée
            anomaly_type = "NO_UPDATE"
        elif bike_stands > 0 and available_bikes == bike_stands:
            # autant de vélos que d'emplacements → station saturée, plus possible d'en déposer
            anomaly_type = "FULL_STANDS"
        elif status == "OPEN" and available_bikes == 0:
            # station ouverte mais aucun vélo disponible → manque à réapprovisionner
            # la consécutivité 2h prévue par l'énoncé sera évaluée globalement par le reducer
            anomaly_type = "ZERO_BIKES"

        # 4 champs séparés par tabulation (format attendu en sortie)
        # .format() utilisé car Python 3.5 sur les conteneurs Hadoop (Debian Stretch)
        print("{}\t{}\t{}\t{}".format(station_id, anomaly_type, last_update, age_last_update))


if __name__ == "__main__":
    main()
