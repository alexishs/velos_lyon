#!/usr/bin/env python3
# Exécuté par : nodemanager (Hadoop Streaming) / dev (test manuel)
"""MR1 reducer — agrège les métriques load_factor par station.
Reçoit du tabulé trié par station_id sur stdin, émet du tabulé sur stdout."""

import math
import sys


def emit(station_id, valid_load_factors, total_samples):
    """Émet le résultat agrégé pour une station."""
    if station_id is None or total_samples == 0:
        return

    nb_valides = len(valid_load_factors)
    if nb_valides > 0:
        avg = sum(valid_load_factors) / nb_valides
        # écart-type de population (variance divisée par n, pas n-1)
        variance = sum((x - avg) ** 2 for x in valid_load_factors) / nb_valides
        std = math.sqrt(variance)
    else:
        avg = 0.0
        std = 0.0

    # .format() utilisé au lieu de f-string : Python 3.5 sur les conteneurs Hadoop (Debian Stretch)
    print("{}\t{:.4f}\t{:.4f}\t{}\t{}".format(station_id, avg, std, nb_valides, total_samples))


def main():
    # Hadoop garantit que les lignes arrivent triées par clé (station_id) au reducer.
    # On agrège tant que la clé reste la même, on émet au changement de clé.
    current_station = None
    valid_load_factors = []
    total_samples = 0

    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 4:
            continue  # ligne mal formée → ignorée

        station_id, _timestamp, load_factor_str, status_valide_str = parts

        # changement de clé : on émet le résultat de la station précédente puis on reset
        if station_id != current_station:
            emit(current_station, valid_load_factors, total_samples)
            current_station = station_id
            valid_load_factors = []
            total_samples = 0

        total_samples += 1
        # seuls les échantillons valides comptent pour avg et std (contrainte énoncé)
        if status_valide_str == "1":
            try:
                valid_load_factors.append(float(load_factor_str))
            except ValueError:
                continue

    # dernière station (pas de changement de clé pour déclencher l'émission)
    emit(current_station, valid_load_factors, total_samples)


if __name__ == "__main__":
    main()
