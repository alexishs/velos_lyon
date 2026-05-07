#!/usr/bin/env python3
# Exécuté par : nodemanager (Hadoop Streaming) / dev (test manuel)
"""MR3 reducer — calcule le 95e percentile du load_factor, le nombre de stations
et la capacité totale par couple (heure, quartier).

Hadoop Streaming trie par défaut sur le premier champ (la clé). Avec une clé
composite (heure, quartier), les enregistrements pour la même heure peuvent
arriver dans n'importe quel ordre de quartier. On agrège donc en mémoire avec
un dictionnaire indexé par (heure, quartier), puis on émet à la fin du flux.

Volume de données acceptable pour cette approche : ~24 heures × 9 quartiers
= 216 groupes max, chacun avec quelques centaines d'observations.
"""

import math
import sys


def percentile(sorted_values, p):
    """95e percentile via la méthode "nearest rank" (tri + position p × n).
    Les valeurs en entrée doivent déjà être triées."""
    if not sorted_values:
        return 0.0
    # rang = ceil(p * n) - 1, clampé dans les bornes du tableau
    idx = math.ceil(p * len(sorted_values)) - 1
    idx = max(0, min(idx, len(sorted_values) - 1))
    return sorted_values[idx]


def main():
    # agrégation en mémoire : { (heure, quartier): { "loads": [...], "capacities": [...] } }
    groups = {}

    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 4:
            continue  # ligne mal formée → ignorée

        heure, quartier, load_factor_str, capacite_str = parts

        try:
            load_factor = float(load_factor_str)
            capacite = int(capacite_str)
        except ValueError:
            continue

        key = (heure, quartier)
        if key not in groups:
            groups[key] = {"loads": [], "capacities": []}
        groups[key]["loads"].append(load_factor)
        groups[key]["capacities"].append(capacite)

    # émission triée par (heure, quartier) pour une sortie déterministe
    for (heure, quartier), data in sorted(groups.items()):
        loads = data["loads"]
        capacities = data["capacities"]

        if not loads:
            continue

        loads_sorted = sorted(loads)
        p95 = percentile(loads_sorted, 0.95)

        nb_stations = len(loads)
        capacite_totale = sum(capacities)

        # .format() utilisé car Python 3.5 sur les conteneurs Hadoop (Debian Stretch)
        print("{}\t{}\t{:.4f}\t{}\t{}".format(
            heure, quartier, p95, nb_stations, capacite_totale
        ))


if __name__ == "__main__":
    main()
