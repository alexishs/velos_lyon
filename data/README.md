# Données de référence

Ce répertoire contient les données externes utilisées par le pipeline qui ne sont pas de la donnée temps réel.

## arrondissements-lyon.geojson

Contours géographiques officiels des 9 arrondissements de la ville de Lyon.

- **Source** : [data.gouv.fr — Arrondissements de la commune de Lyon](https://www.data.gouv.fr/fr/datasets/arrondissements-de-la-commune-de-lyon/)
- **Producteur** : Métropole de Lyon (Grand Lyon)
- **Format** : GeoJSON (FeatureCollection, MultiPolygon par arrondissement)
- **Projection** : EPSG:4326 (WGS84, lat/lng standard)
- **Utilisé par** : [step05_mr3_mapper_horaire.py](../step05_mr3_mapper_horaire.py) pour le rattachement des stations Vélo'v à un arrondissement par test point-in-polygon.

### Mise à jour du fichier

```bash
curl -sL "https://data.grandlyon.com/geoserver/metropole-de-lyon/ows?SERVICE=WFS&VERSION=2.0.0&request=GetFeature&typename=metropole-de-lyon:adr_voie_lieu.adrarrond&outputFormat=application/json&SRSNAME=EPSG:4326" \
  -o data/arrondissements-lyon.geojson
```

L'URL est extraite du champ `resources` retourné par l'API data.gouv.fr (`https://www.data.gouv.fr/api/1/datasets/arrondissements-de-la-commune-de-lyon/`).

### Limites couvertes

Le fichier ne couvre que les **9 arrondissements de Lyon intra-muros**. Les stations Vélo'v du contrat JCDecaux `lyon` situées en banlieue (Villeurbanne, Vénissieux, Caluire, etc.) tombent en dehors de ces polygones et sont étiquetées `Hors_Lyon` par le mapper MR3.
