#!/usr/bin/env python3
# Exécuté par : nodemanager (Hadoop Streaming) / dev (test manuel)
"""MR4 reducer — agrège les utilisations par arrondissement et calcule le CA
potentiel total et la priorité d'investissement.

Reçoit en stdin les lignes émises par le mapper (quartier, utilisation%,
ca_potentiel€, nom_station, bike_stands), triées par quartier par Hadoop.
Émet pour chaque quartier : nb_stations, capacite_totale, ca_potentiel_total,
priorite (1 si CA > 500k€, 0 sinon).
"""

import sys


def emit(quartier, nb_stations, capacite_totale, ca_potentiel_total):
    """Émet le résultat agrégé pour un quartier avec formatage humain du CA."""
    if quartier is None or nb_stations == 0:
        return

    # priorité d'investissement : seuil de 500 000 € de CA potentiel annuel
    priorite = 1 if ca_potentiel_total > 500_000 else 0

    # affichage compact : 2.1M€ pour les valeurs ≥ 1M, 740k€ entre 1k et 1M, sinon en €
    if ca_potentiel_total >= 1_000_000:
        ca_str = "{:.1f}M€".format(ca_potentiel_total / 1_000_000)
    elif ca_potentiel_total >= 1_000:
        ca_str = "{:.0f}k€".format(ca_potentiel_total / 1_000)
    else:
        ca_str = "{:.0f}€".format(ca_potentiel_total)

    # .format() utilisé car Python 3.5 sur les conteneurs Hadoop (Debian Stretch)
    print("{}\t{}\t{}\t{}\t{}".format(
        quartier, nb_stations, capacite_totale, ca_str, priorite
    ))


def main():
    # Hadoop garantit que les lignes arrivent triées par clé (quartier) au reducer.
    # On agrège tant que la clé reste la même, on émet au changement.
    current_quartier = None
    nb_stations = 0
    capacite_totale = 0
    ca_potentiel_total = 0.0

    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) != 5:
            continue  # ligne mal formée → ignorée

        quartier, utilisation_str, ca_str, _nom, bike_stands_str = parts

        # parsing des valeurs typées : "87%" → 87, "15930€" → 15930
        try:
            ca_potentiel = float(ca_str.rstrip("€"))
            bike_stands = int(bike_stands_str)
        except ValueError:
            continue

        # changement de clé : on émet le résultat du quartier précédent puis on reset
        if quartier != current_quartier:
            emit(current_quartier, nb_stations, capacite_totale, ca_potentiel_total)
            current_quartier = quartier
            nb_stations = 0
            capacite_totale = 0
            ca_potentiel_total = 0.0

        nb_stations += 1
        capacite_totale += bike_stands
        ca_potentiel_total += ca_potentiel

    # dernier quartier (pas de changement de clé pour déclencher l'émission)
    emit(current_quartier, nb_stations, capacite_totale, ca_potentiel_total)


if __name__ == "__main__":
    main()
