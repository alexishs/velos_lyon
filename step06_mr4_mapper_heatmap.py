#!/usr/bin/env python3
# Exécuté par : nodemanager (Hadoop Streaming) / dev (test manuel)
"""MR4 mapper — calcule par station son utilisation moyenne et son CA potentiel,
et l'émet avec son arrondissement et son nom.

Schéma d'entrée : API JCDecaux v1 (number, name, position, bike_stands,
available_bikes, available_bike_stands).

Particularité : la sortie est PAR STATION (un avg_load_factor par station),
pas par snapshot — c'est ce que demande l'énoncé MR4.
Le mapper bufferise donc tous ses inputs en mémoire pour faire l'agrégation.
Acceptable pour le volume de ce TP (~250 stations × N snapshots = quelques
milliers de records). Hypothèse : Hadoop Streaming utilise un seul mapper
sur ces données (vrai par défaut vu la taille des fichiers, < 128 Mo).

Détermination de l'arrondissement : même mécanisme que MR3 (point-in-polygon
sur le GeoJSON officiel data.gouv.fr), code dupliqué pour autonomie de chaque
mapper en environnement Hadoop Streaming.

Format de sortie : 5 champs (la spec énoncé en mentionne 4, on ajoute
`bike_stands` en 5e position pour que le reducer puisse calculer
`capacite_totale` sans back-calcul fragile).
"""

import json
import os
import re
import sys

GEOJSON_FILENAME = "arrondissements-lyon.geojson"


def find_geojson():
    """Recherche du fichier GeoJSON selon le contexte d'exécution (cf. MR3)."""
    for path in (GEOJSON_FILENAME, os.path.join("data", GEOJSON_FILENAME), os.path.join("/workspace/data", GEOJSON_FILENAME)):
        if os.path.exists(path):
            return path
    raise FileNotFoundError("GeoJSON arrondissements introuvable")


def load_arrondissements(path):
    """Charge le GeoJSON et retourne [(nom_court, multi_coords), ...]."""
    with open(path) as f:
        geojson = json.load(f)
    result = []
    for feature in geojson["features"]:
        nom_long = feature["properties"]["nom"]
        m = re.match(r"Lyon\s+(\d+)", nom_long)
        nom_court = "Lyon{}".format(m.group(1)) if m else nom_long.replace(" ", "_")
        result.append((nom_court, feature["geometry"]["coordinates"]))
    return result


def point_in_ring(x, y, ring):
    """Ray-casting : point dans un anneau polygonal fermé."""
    n = len(ring)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def get_quartier(lat, lng, arrondissements):
    """Retourne l'arrondissement contenant (lat, lng) ou 'Hors_Lyon'."""
    # GeoJSON utilise (lng, lat), pas (lat, lng) — vigilance à l'inversion
    for name, multi_coords in arrondissements:
        for polygon in multi_coords:
            if point_in_ring(lng, lat, polygon[0]):
                return name
    return "Hors_Lyon"


def main():
    arrondissements = load_arrondissements(find_geojson())

    # buffer en mémoire : station_id → infos cumulées
    stations = {}

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            station = json.loads(line)
        except json.JSONDecodeError:
            continue  # contrainte énoncé : JSON malformé → ligne ignorée

        try:
            sid = station["number"]
            name = station["name"]
            bike_stands = station["bike_stands"]
            available_bikes = station["available_bikes"]
            available_stands = station["available_bike_stands"]
            lat = station["position"]["lat"]
            lng = station["position"]["lng"]
        except (KeyError, TypeError):
            continue

        # load_factor instantané pour cette observation
        total = available_bikes + available_stands
        load_factor = available_bikes / total if total > 0 else 0.0

        # première observation d'une station : on capture son contexte fixe
        # (nom, arrondissement, capacité) puis on accumule les load_factors
        if sid not in stations:
            stations[sid] = {
                "name": name,
                "bike_stands": bike_stands,
                "quartier": get_quartier(lat, lng, arrondissements),
                "load_factors": [],
            }
        stations[sid]["load_factors"].append(load_factor)

    # émission : une ligne par station, avec utilisation moyenne et CA annuel
    for sid, data in stations.items():
        load_factors = data["load_factors"]
        if not load_factors:
            continue

        # avg_load_factor (fraction 0-1), comme dans MR1
        avg = sum(load_factors) / len(load_factors)
        utilisation = avg * 100  # en pourcentage 0-100 par convention de l'énoncé

        # ca_potentiel = avg × capacité × 2€/jour × 365jours
        # (utilisation en fraction, le ×100 du % est conventionnel pour l'affichage)
        ca_potentiel = avg * data["bike_stands"] * 2 * 365

        # 5 champs (cf. docstring : 5e champ bike_stands ajouté pour le reducer).
        # Format : quartier, utilisation%, ca_potentiel€, nom, capacité
        # .format() utilisé car Python 3.5 sur les conteneurs Hadoop (Debian Stretch).
        print("{}\t{:.0f}%\t{:.0f}€\t{}\t{}".format(
            data["quartier"], utilisation, ca_potentiel, data["name"], data["bike_stands"]
        ))


if __name__ == "__main__":
    main()
