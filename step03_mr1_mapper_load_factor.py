#!/usr/bin/env python3
# Exécuté par : nodemanager (Hadoop Streaming) / dev (test manuel)
"""MR1 mapper — calcule load_factor et status_valide pour chaque station.
Lit du JSONL sur stdin, émet des lignes tabulées sur stdout."""

import json
import sys


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            station = json.loads(line)
        except json.JSONDecodeError:
            continue  # contrainte énoncé : JSON malformé → ligne ignorée (pas crash)

        try:
            station_id = station["number"]
            available_bikes = station["available_bikes"]
            available_stands = station["available_bike_stands"]
            bike_stands = station["bike_stands"]
            status = station["status"]
            # last_update vient en millisecondes (epoch ms), on convertit en secondes
            timestamp = station["last_update"] // 1000
        except (KeyError, TypeError):
            continue  # champ manquant ou type invalide → ligne ignorée

        # load_factor : ratio de vélos disponibles sur la capacité opérationnelle.
        # On utilise (available_bikes + available_bike_stands) plutôt que bike_stands :
        # certaines bornes peuvent être HS et sont alors exclues du dénominateur.
        total = available_bikes + available_stands
        load_factor = available_bikes / total if total > 0 else 0.0

        # status_valide : 1 si station opérationnelle et compteurs cohérents avec la capacité
        is_valid = 1 if status == "OPEN" and 0 <= available_bikes <= bike_stands else 0

        # 4 champs séparés par tabulation (format attendu en sortie)
        # .format() utilisé au lieu de f-string : Python 3.5 sur les conteneurs Hadoop (Debian Stretch)
        print("{}\t{}\t{:.4f}\t{}".format(station_id, timestamp, load_factor, is_valid))


if __name__ == "__main__":
    main()
