#!/usr/bin/env python3
# Exécuté par : nodemanager (Hadoop Streaming) / dev (test manuel)
"""MR2 reducer — agrège les anomalies par station et calcule un score de fiabilité.

Reçoit en stdin les lignes émises par le mapper, triées par station_id par Hadoop.
Émet pour chaque station : station_id, fiabilité (%), nombre d'anomalies, type de
la panne la plus récente."""

import sys


def emit(station_id, total_samples, nb_anomalies, derniere_panne):
    """Émet le résultat agrégé pour une station."""
    if station_id is None or total_samples == 0:
        return

    # formule énoncé : ratio des relevés sains sur le total
    fiabilite = (total_samples - nb_anomalies) / total_samples * 100
    # si aucune anomalie détectée → on émet "AUCUNE" plutôt qu'une chaîne vide
    derniere_panne_str = derniere_panne if derniere_panne else "AUCUNE"

    # .format() utilisé car Python 3.5 sur les conteneurs Hadoop (Debian Stretch)
    print("{}\t{:.1f}%\t{}\t{}".format(
        station_id, fiabilite, nb_anomalies, derniere_panne_str
    ))


def main():
    # Hadoop garantit que les lignes arrivent triées par clé (station_id) au reducer.
    # On agrège tant que la clé reste la même, on émet au changement de clé.
    current_station = None
    total_samples = 0
    nb_anomalies = 0
    derniere_panne = None
    derniere_panne_ts = 0  # timestamp epoch s, pour identifier la panne la plus récente

    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 4:
            continue  # ligne mal formée → ignorée

        station_id, anomaly_type, timestamp_str, _age = parts

        # changement de clé : on émet le résultat de la station précédente puis on reset
        if station_id != current_station:
            emit(current_station, total_samples, nb_anomalies, derniere_panne)
            current_station = station_id
            total_samples = 0
            nb_anomalies = 0
            derniere_panne = None
            derniere_panne_ts = 0

        total_samples += 1

        # toute valeur autre que "OK" est une anomalie à comptabiliser
        if anomaly_type != "OK":
            nb_anomalies += 1
            try:
                ts = int(timestamp_str)
            except ValueError:
                ts = 0
            # garder la panne la plus récente (timestamp le plus élevé)
            if ts >= derniere_panne_ts:
                derniere_panne_ts = ts
                derniere_panne = anomaly_type

    # dernière station (pas de changement de clé pour déclencher l'émission)
    emit(current_station, total_samples, nb_anomalies, derniere_panne)


if __name__ == "__main__":
    main()
