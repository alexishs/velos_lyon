#!/usr/bin/env python3
# Exécuté par : nodemanager (Hadoop Streaming) / dev (test manuel)
"""MR3 mapper — émet pour chaque station observée son créneau horaire,
son arrondissement, son load_factor et sa capacité.

Schéma d'entrée : API JCDecaux v1 (number, last_update en ms, available_bikes,
available_bike_stands, bike_stands, position {lat, lng}).

Détermination de l'arrondissement par point-in-polygon sur les contours officiels
fournis par data.gouv.fr (dataset "Arrondissements de la commune de Lyon", source
Grand Lyon, EPSG:4326). Le fichier GeoJSON est embarqué dans le projet sous
data/arrondissements-lyon.geojson et distribué aux nodemanagers via -files.
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

GEOJSON_FILENAME = "arrondissements-lyon.geojson"


def find_geojson():
    """Recherche du fichier GeoJSON dans les emplacements possibles selon le contexte d'exécution."""
    candidates = [
        # 1. Cwd de la tâche Hadoop Streaming (fichier shippé via -files)
        GEOJSON_FILENAME,
        # 2. Répertoire data/ pour exécution locale dans le conteneur dev depuis /workspace
        os.path.join("data", GEOJSON_FILENAME),
        # 3. Chemin absolu vers /workspace/data (autre cas d'exécution dans dev)
        os.path.join("/workspace/data", GEOJSON_FILENAME),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(
        "GeoJSON arrondissements introuvable, cherché dans : {}".format(candidates)
    )


def load_arrondissements(path):
    """Charge le GeoJSON et retourne une liste de tuples (nom_court, multi_coordonnees).

    Le nom court est de la forme 'LyonN' (ex : 'Lyon1') au lieu du libellé long
    'Lyon 1er Arrondissement', plus pratique en sortie tabulée.
    """
    with open(path) as f:
        geojson = json.load(f)

    result = []
    for feature in geojson["features"]:
        nom_long = feature["properties"]["nom"]
        # extraction du numéro d'arrondissement : "Lyon 1er Arrondissement" → "Lyon1"
        m = re.match(r"Lyon\s+(\d+)", nom_long)
        nom_court = "Lyon{}".format(m.group(1)) if m else nom_long.replace(" ", "_")

        # le GeoJSON stocke les MultiPolygon comme une liste de polygones,
        # chaque polygone étant une liste de rings (1er ring = contour externe)
        multi_coords = feature["geometry"]["coordinates"]
        result.append((nom_court, multi_coords))

    return result


def point_in_ring(x, y, ring):
    """Algorithme ray-casting : compte les intersections d'un rayon horizontal
    partant du point avec les arêtes du polygone. Nombre impair → point intérieur.

    GeoJSON encode les coordonnées en [longitude, latitude], donc x = lng, y = lat.
    """
    n = len(ring)
    inside = False
    # j est l'index du sommet précédent (pour former l'arête courante)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        # condition classique du ray-casting : l'arête traverse-t-elle la ligne y ?
        # et le point d'intersection est-il à droite de notre point ?
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def point_in_multipolygon(x, y, multi_coords):
    """Test point-in-polygon pour un MultiPolygon : True si le point est dans
    au moins une des composantes (en n'examinant que le ring extérieur)."""
    for polygon in multi_coords:
        # polygon[0] est le ring extérieur ; les éventuels trous (polygon[1:]) sont
        # ignorés ici car les arrondissements lyonnais n'en ont pas en pratique
        outer_ring = polygon[0]
        if point_in_ring(x, y, outer_ring):
            return True
    return False


def get_quartier(lat, lng, arrondissements):
    """Retourne le nom de l'arrondissement contenant (lat, lng) ou 'Hors_Lyon'
    si le point n'est dans aucun polygone (cas marginal : station en limite)."""
    # GeoJSON utilise (lng, lat), pas (lat, lng) — vigilance à l'inversion
    for name, multi_coords in arrondissements:
        if point_in_multipolygon(lng, lat, multi_coords):
            return name
    return "Hors_Lyon"


def main():
    # chargement unique du GeoJSON au démarrage du mapper (réutilisé pour toutes les lignes)
    arrondissements = load_arrondissements(find_geojson())

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            station = json.loads(line)
        except json.JSONDecodeError:
            continue  # contrainte énoncé : JSON malformé → ligne ignorée

        try:
            available_bikes = station["available_bikes"]
            available_stands = station["available_bike_stands"]
            bike_stands = station["bike_stands"]
            # last_update vient en millisecondes epoch → conversion en secondes
            last_update = station["last_update"] // 1000
            lat = station["position"]["lat"]
            lng = station["position"]["lng"]
        except (KeyError, TypeError):
            continue  # champ manquant ou type invalide → ligne ignorée

        # extraction de l'heure UTC (0-23) à partir du timestamp last_update
        heure = datetime.fromtimestamp(last_update, tz=timezone.utc).hour

        quartier = get_quartier(lat, lng, arrondissements)

        # load_factor calculé comme dans MR1 : ratio sur capacité opérationnelle
        # (exclut les bornes HS du dénominateur)
        total = available_bikes + available_stands
        load_factor = available_bikes / total if total > 0 else 0.0

        # 4 champs séparés par tabulation, format heure sur 2 chiffres pour tri lexical correct.
        # .format() utilisé car Python 3.5 sur les conteneurs Hadoop (Debian Stretch).
        print("{:02d}\t{}\t{:.4f}\t{}".format(heure, quartier, load_factor, bike_stands))


if __name__ == "__main__":
    main()
