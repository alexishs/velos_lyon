# TP : Data Lake Vélo Lyon — Pipeline temps réel avec Hadoop / MapReduce / Kafka / Hive

**Durée** : 1 semaine et demi | **Groupe** : 3/4 organisation en groupe mais travail individuel

## Contexte métier fictif : Vélo Lyon

Vélo Lyon (250 stations, 2500 vélos, 15M€ investis) traverse une **crise de service critique** :

- **87% des plaintes usagers** : "Aucune station avec vélo disponible autour de moi"
- **+42% coûts réapprovisionnement** : 18 camions/jour en urgence heure de pointe
- **32% vélos inutilisés** : bloqués périphérie matin, centre-ville désert soir
- **Objectif politique 2027** : 50% trajets <5km en vélo (actuel : 14%)

**Exemple station critique** (API JCDecaux Lyon) :

> Station n°2010 - CONFLUENCE/DARSE : 22 places, 0 vélo disponible, 21 places vides
> → 150m du métro → 80 usagers/jour impactés → 2€/jour/vélo perdu = 44€/jour

**Stakeholders à satisfaire** :

1. **Exploitation** : "Où envoyer les camions dans 15min ?"
2. **Maintenance** : "Quelles bornes sont HS avant les appels clients ?"
3. **Direction** : "Où investir 5M€ pour 500 nouveaux vélos ?"

**Mission** : Construire un data lake **from scratch** qui transforme ces données brutes en datasets prêts pour le décisionnel, en utilisant Hadoop / MapReduce / Kafka / Hive.

## Étapes

### 1. Installation de l'environnement Hadoop

Installer un cluster Hadoop en environnement de développement/test.

### 2. Reproduction de l'arborescence HDFS

Recréer une structure des données en production sur HDFS.

**2.1** Créer les répertoires HDFS nécessaires (`/data-lake/raw`, `/data-lake/processed`, `/data-lake/analytics`).

Les données en temps réel : [JCDecaux Open Data](https://developer.jcdecaux.com/#/opendata/vls?page=getstarted)

## Jobs MapReduce Python — Spécifications détaillées

### MR1 : Calcul load factor + validation

#### Objectif métier

Produire la métrique `load_factor` par station pour décider où envoyer les camions de réapprovisionnement, et la métrique `status_valide` pour s'assurer de la validité des informations recueillies.

#### Spécifications techniques

**Mapper (`mapper_load_factor.py`)** :

```
INPUT  : 1 JSON/station par ligne stdin
OUTPUT : station_id|timestamp|load_factor|status_valide
```

Calculs :
- `load_factor` = available_bikes ÷ (available_bikes + available_bike_stands)
- `status_valide` = 1 si (status=OPEN et 0 ≤ available_bikes ≤ bike_stands) sinon 0

Format sortie (tabulation) : `"station_id\t1712473200\t0.045\t1"`

**Reducer (`reducer_load_factor.py`)** :

```
INPUT  : station_id|timestamp|load_factor|status_valide
OUTPUT : station_id|avg_load_factor|std_load|nb_samples_valides|total_samples
```

Agrégations (sur échantillons `status_valide=1` uniquement) :
- `avg_load_factor`
- `std_load` (écart-type load_factor)
- `nb_samples_valides` / `total_samples`

L'agrégation se calcule sur une fenêtre temporelle définie (exemple : sur une heure pour chaque minute).

**Commande Hadoop Streaming** :

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-<version>.jar \
  -input /data-lake/raw/velo_lyon/* \
  -output /data-lake/processed/load_metrics/ \
  -D stream.map.output.field.separator=\\t \
  -mapper mapper.py \
  -file mapper.py \
  -reducer reducer.py \
  -file reducer.py
```

Référence : https://hadoop.apache.org/docs/r1.2.1/streaming.html

**Sortie HDFS attendue** `/data-lake/processed/load_metrics/part-r-00000` :

```
station_id	0.23	0.15	142/180
station_id	0.78	0.09	156/180
```

---

### MR2 : Détection anomalies bornes/capteurs

#### Objectif métier

Alerter l'équipe maintenance avant que les usagers ne se plaignent, en identifiant trois types de dysfonctionnements : absence de mise à jour depuis plus de 30 min (`NO_UPDATE`), station ouverte mais vide depuis plus de 2h (`ZERO_BIKES`), station systématiquement pleine (`FULL_STANDS`). Le résultat est un score de fiabilité par station.

#### Spécifications techniques

**Mapper (`mapper_anomalies.py`)** :

```
INPUT  : JSON brut
OUTPUT : station_id|anomaly_type|timestamp|age_last_update
```

3 anomalies détectées :
1. `NO_UPDATE` si last_update > 30min
2. `ZERO_BIKES` si status=OPEN et available_bikes=0 > 2h consécutifs
3. `FULL_STANDS` si available_bike_stands = bike_stands

Exemple : `"2010\tNO_UPDATE\t1712473200\t7800"`

**Reducer (`reducer_anomalies.py`)** :

```
INPUT  : station_id|anomaly_type|timestamp|age_last_update
OUTPUT : station_id|fiabilite_pourcent|nb_anomalies|derniere_panne
```

`fiabilite` = (total_samples - nb_anomalies) ÷ total_samples × 100

**Sortie attendue** :

```
2010	67%	12	NO_UPDATE
2025	92%	3	ZERO_BIKES
```

---

### MR3 : Agrégats horaire/quartier

#### Objectif métier

Prédire les pics de demande par quartier/heure.

#### Spécifications techniques

**Mapper (`mapper_horaire.py`)** :

```
INPUT  : JSON brut
OUTPUT : heure|quartier|load_factor|capacite
```

- `heure` = last_update heure (00-23)
- `quartier` = fonction lat/lng → Lyon1/2/3/7/9 (heuristique à coder depuis [data.gouv.fr](https://www.data.gouv.fr/datasets/arrondissements-de-la-commune-de-lyon))

Exemple : `"18\tLyon2\t0.87\t22"`

**Reducer (`reducer_horaire.py`)** :

```
INPUT  : heure|quartier|load_factor|capacite
OUTPUT : heure|quartier|p95_load|nb_stations|capacite_totale
```

`p95_load` = 95e percentile load_factor (trier + position 95%)

**Sortie attendue** :

```
18	Lyon2	0.92	45	990
19	Lyon7	0.76	32	704
```

---

### MR4 : Heatmap stratégique

#### Objectif métier

Identifier 20 stations "sous-équipées" pour 5M€ d'investissement.

#### Spécifications techniques

**Mapper (`mapper_heatmap.py`)** :

```
INPUT  : JSON brut
OUTPUT : quartier|utilisation|ca_potentiel|nom_station
```

- `utilisation` = avg_load_factor (MR1) × 100
- `ca_potentiel` = utilisation × bike_stands × 2€/jour × 365

Exemple : `"Lyon2\t87%\t15930€\tCONFLUENCE"`

**Reducer (`reducer_heatmap.py`)** :

```
INPUT  : quartier|utilisation|ca_potentiel|nom_station
OUTPUT : quartier|stations_nb|capacite_totale|ca_potentiel_total|priorite
```

`priorite` = 1 si ca_potentiel_total > 500k€ sinon 0

**Sortie attendue** :

```
Lyon2	45	990	2.1M€	1
Lyon7	32	704	1.4M€	1
```

---

### Vérification de chaque job

```bash
# Après chaque MR :
hdfs dfs -cat /data-lake/processed/[nom_job]/part-r-00000 | head -5

# Validation mapper seul :
cat sample.json | python mapper_*.py | head -3
```

### Contraintes techniques

- ✅ Mapper : JSON stdin → tabulation stdout (pas JSON en sortie)
- ✅ Reducer : tabulations stdin → tabulation stdout
- ✅ Gestion erreurs : JSON malformé → ligne ignorée (pas crash)
- ✅ Partition HDFS : 4-8 fichiers/job max (coalesce natif streaming)
- ✅ Encodage : UTF-8 partout

## Questions métier à répondre via des tables Hive

- **Q1** — Quelles sont les 15 stations qui n'ont plus aucun vélo disponible en ce moment, en commençant par les plus grandes ?
- **Q2** — Quels sont les 5 quartiers les plus en tension, en croisant le nombre de stations vides et leur capacité totale ?
- **Q3** — Le nombre de vélos disponibles à la station 2010 a-t-il augmenté ou diminué ces 10 dernières minutes ?
- **Q4** — Quelles stations n'ont pas envoyé de données depuis plus de 2 heures, ou sont restées à zéro vélo pendant 4 heures d'affilée ?
- **Q5** — Quels arrondissements affichent un taux de remplissage supérieur à 85% avec moins de 15 places libres en moyenne ?
- **Q6** — Quelle proportion des stations dispose de coordonnées GPS et d'un statut valides ?

## Stack technique

```
🚲 API JCDecaux Lyon (JSON 60s)
        ↓
📨 Kafka Producer Python → topic velo_lyon_raw
        ↓
💾 HDFS raw → /data-lake/raw/velo_lyon/YYYY-MM-DD-HH/
        ↓
🔄 Hadoop Streaming MapReduce Python (4 jobs)
   ├── MR1 : calcul load_factor + validation
   ├── MR2 : détection anomalies bornes/capteurs
   ├── MR3 : agrégats horaire/quartier
   └── MR4 : heatmap stratégique
        ↓
🗄️ Hive External Tables (3 tables) → 6 requêtes métiers
```

- ✅ OPTIONNEL : Kafka ou interrogation API REST
- ✅ OBLIGATOIRE : Hadoop Streaming Python (mapper/reducer `.py`)
- ✅ OBLIGATOIRE : HDFS JSON raw → processed → curated
- ✅ OBLIGATOIRE : Hive EXTERNAL tables
- ✅ OPTIONNEL : PySpark

**API clé gratuite** : https://developer.jcdecaux.com/

## Livrables

```
tp-velo-lyon/
├── docker-compose.yml           # Kafka + Hadoop 3.x
├── mapper_load_factor.py        # MR1
├── reducer_load_factor.py       # MR1
├── mapper_anomalies.py          # MR2
├── reducer_anomalies.py         # MR2
├── mapper_horaire.py            # MR3
├── mapper_quartier.py           # MR4
├── create_hive_tables.sql       # tables Hive
├── requetes_metier.sql          # 6 questions + résultats
├── hdfs_inventory.txt           # hdfs dfs -ls -R /data-lake
├── architecture.png             # Kafka→HDFS→MR→Hive
├── atlas_entities.json          # Déclaration des entités via l'API REST Atlas
└── lineage_screenshot.png       # Capture du graphe de lineage dans l'UI Atlas
```

**Rendu** : `tp-velo-lyon-[nom1-nom2].zip`

### Apache Atlas — Gouvernance des données

Une fois le pipeline opérationnel, référencer les datasets dans Apache Atlas pour garantir la traçabilité et la gouvernance.

Pour chacun des 3 datasets du data lake (`raw_velo_stations`, `processed_load_factor`, `curated_quartiers`), créer une entrée dans Atlas avec :
- Nom, description, source d'alimentation, fréquence de mise à jour, format, chemin HDFS, classification RGPD
- **Sensibilité RGPD** : les données de stations sont publiques — à justifier explicitement
- **Cycle de vie** : durée de rétention par zone (raw 30j / processed 7j / curated permanent)
- **Lineage** : tracer la chaîne API → HDFS raw → MR1 → processed → Hive
