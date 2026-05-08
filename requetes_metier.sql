-- Requêtes métier sur les 3 tables Hive External.
-- Adressent les 6 questions Q1 à Q6 de l'énoncé.
--
-- Exécution (après create_hive_tables.sql) :
--   beeline -u jdbc:hive2://hive-server:10000 -f requetes_metier.sql

-- Activation de la lecture récursive nécessaire pour raw_velo_stations
-- (les fichiers JSONL sont organisés en sous-répertoires par heure).
SET hive.mapred.supports.subdirectories=true;
SET mapreduce.input.fileinputformat.input.dir.recursive=true;

-- Q3 et Q4 utilisent des cross-joins implicites avec une CTE constante (1 ligne)
-- pour propager la borne temporelle "now". Hive 2.x bloque cela en mode strict
-- alors que dans ce cas précis le cross product est volontaire et borné.
SET hive.strict.checks.cartesian.product=false;
SET hive.mapred.mode=nonstrict;

-- ============================================================================
-- Q1 — Quelles sont les 15 stations qui n'ont plus aucun vélo disponible
--      en ce moment, en commençant par les plus grandes ?
--
-- Approche : ROW_NUMBER() partitionné par station prend exactement une
-- observation (la plus récente) par station. Le filtre OPEN exclut les stations
-- fermées (un statut CLOSED rendrait l'absence de vélo non pertinente).
-- ============================================================================

WITH dernier_par_station AS (
  SELECT
    number,
    name,
    bike_stands,
    available_bikes,
    status,
    ROW_NUMBER() OVER (PARTITION BY number ORDER BY last_update DESC) AS rn
  FROM raw_velo_stations
)
SELECT
  number AS station_id,
  name,
  bike_stands AS capacite
FROM dernier_par_station
WHERE rn = 1
  AND available_bikes = 0
  AND status = 'OPEN'
ORDER BY capacite DESC
LIMIT 15;

-- ============================================================================
-- Q2 — Quels sont les 5 quartiers les plus en tension, en croisant le nombre
--      de stations vides et leur capacité totale ?
--
-- Métrique de tension : nb_stations_vides × capacite_totale
-- (plus il y a de stations vides ET de capacité, plus le quartier souffre).
--
-- Approximation : le quartier est inféré par bounding box ordonnée des plus
-- petites aux plus grandes pour limiter les chevauchements (Lyon1 d'abord).
-- Les bboxes proviennent du GeoJSON officiel data.gouv.fr (cf. data/README.md).
-- ============================================================================

WITH stations_courantes AS (
  SELECT
    number,
    bike_stands,
    available_bikes,
    -- bbox prioritaire de la plus petite à la plus grande pour limiter les
    -- chevauchements (premier match gagne). Source : extraction depuis le
    -- GeoJSON officiel data.gouv.fr (cf. data/README.md).
    CASE
      WHEN position.lat BETWEEN 45.7645 AND 45.7748
       AND position.lng BETWEEN 4.8131 AND 4.8398 THEN 'Lyon1'
      WHEN position.lat BETWEEN 45.7711 AND 45.7897
       AND position.lng BETWEEN 4.8090 AND 4.8430 THEN 'Lyon4'
      WHEN position.lat BETWEEN 45.7636 AND 45.7871
       AND position.lng BETWEEN 4.8394 AND 4.8699 THEN 'Lyon6'
      WHEN position.lat BETWEEN 45.7390 AND 45.7637
       AND position.lng BETWEEN 4.8384 AND 4.8984 THEN 'Lyon3'
      WHEN position.lat BETWEEN 45.7442 AND 45.7680
       AND position.lng BETWEEN 4.7718 AND 4.8305 THEN 'Lyon5'
      WHEN position.lat BETWEEN 45.7188 AND 45.7494
       AND position.lng BETWEEN 4.8476 AND 4.8921 THEN 'Lyon8'
      WHEN position.lat BETWEEN 45.7074 AND 45.7568
       AND position.lng BETWEEN 4.8187 AND 4.8600 THEN 'Lyon7'
      WHEN position.lat BETWEEN 45.7587 AND 45.8083
       AND position.lng BETWEEN 4.7840 AND 4.8412 THEN 'Lyon9'
      WHEN position.lat BETWEEN 45.7265 AND 45.7663
       AND position.lng BETWEEN 4.8130 AND 4.8398 THEN 'Lyon2'
      ELSE 'Hors_Lyon'
    END AS quartier,
    ROW_NUMBER() OVER (PARTITION BY number ORDER BY last_update DESC) AS rn
  FROM raw_velo_stations
)
SELECT
  quartier,
  SUM(CASE WHEN available_bikes = 0 THEN 1 ELSE 0 END) AS nb_stations_vides,
  SUM(bike_stands) AS capacite_totale,
  SUM(CASE WHEN available_bikes = 0 THEN 1 ELSE 0 END) * SUM(bike_stands) AS tension_score
FROM stations_courantes
WHERE rn = 1
GROUP BY quartier
ORDER BY tension_score DESC
LIMIT 5;

-- ============================================================================
-- Q3 — Le nombre de vélos disponibles à la station 2010 a-t-il augmenté ou
--      diminué ces 10 dernières minutes ?
--
-- Approche : on compare la dernière valeur observée à la valeur la plus ancienne
-- dans la fenêtre des 10 dernières minutes (par rapport au max(last_update) de
-- la station 2010 qui sert de "now"). Les timestamps sont en millisecondes epoch
-- dans raw_velo_stations.
-- ============================================================================

WITH bornes AS (
  SELECT
    MAX(last_update) AS now_ts,
    MAX(last_update) - 10 * 60 * 1000 AS il_y_a_10_min  -- 10 min en millisecondes
  FROM raw_velo_stations
  WHERE number = 2010
),
fenetre AS (
  SELECT
    last_update,
    available_bikes,
    ROW_NUMBER() OVER (ORDER BY last_update ASC) AS rn_asc,
    ROW_NUMBER() OVER (ORDER BY last_update DESC) AS rn_desc
  FROM raw_velo_stations, bornes
  WHERE number = 2010
    AND last_update >= bornes.il_y_a_10_min
)
SELECT
  MAX(CASE WHEN rn_asc = 1 THEN available_bikes END) AS bikes_il_y_a_10_min,
  MAX(CASE WHEN rn_desc = 1 THEN available_bikes END) AS bikes_maintenant,
  MAX(CASE WHEN rn_desc = 1 THEN available_bikes END)
    - MAX(CASE WHEN rn_asc = 1 THEN available_bikes END) AS variation
FROM fenetre;

-- ============================================================================
-- Q4 — Quelles stations n'ont pas envoyé de données depuis plus de 2 heures,
--      ou sont restées à zéro vélo pendant 4 heures d'affilée ?
--
-- Approche en deux UNION :
--   - Volet "no update" : pour chaque station, max(last_update) plus vieux
--     que (max global - 2h). Le max global sert de référence "now".
--   - Volet "zéro 4h" : pour chaque station, on cherche un intervalle de 4h
--     pendant lequel TOUTES les observations ont available_bikes = 0.
--     Approximation : on prend le max et le min des last_update où bikes=0,
--     et on vérifie que l'écart dépasse 4h.
-- ============================================================================

WITH ref_now AS (
  SELECT MAX(last_update) AS now_ts FROM raw_velo_stations
),
no_update AS (
  SELECT
    r.number AS station_id,
    'NO_UPDATE_SUP_2H' AS anomaly,
    (n.now_ts - MAX(r.last_update)) / 1000 / 60 AS minutes_depuis_dernier_envoi
  FROM raw_velo_stations r, ref_now n
  GROUP BY r.number, n.now_ts
  HAVING (n.now_ts - MAX(r.last_update)) / 1000 / 60 > 120
),
zero_4h AS (
  SELECT
    r.number AS station_id,
    'ZERO_BIKES_4H' AS anomaly,
    (MAX(r.last_update) - MIN(r.last_update)) / 1000 / 60 AS duree_zero_minutes
  FROM raw_velo_stations r
  WHERE r.available_bikes = 0 AND r.status = 'OPEN'
  GROUP BY r.number
  HAVING (MAX(r.last_update) - MIN(r.last_update)) / 1000 / 60 > 240
)
SELECT station_id, anomaly, minutes_depuis_dernier_envoi AS duree_minutes FROM no_update
UNION ALL
SELECT station_id, anomaly, duree_zero_minutes FROM zero_4h
ORDER BY duree_minutes DESC;

-- ============================================================================
-- Q5 — Quels arrondissements affichent un taux de remplissage supérieur à 85%
--      avec moins de 15 places libres en moyenne ?
--
-- Approche : on calcule par quartier (bbox CASE WHEN) le load_factor moyen
-- et la moyenne d'available_bike_stands sur le snapshot le plus récent.
-- ============================================================================

WITH stations_courantes AS (
  SELECT
    bike_stands,
    available_bikes,
    available_bike_stands,
    -- même bbox prioritaire que Q2
    CASE
      WHEN position.lat BETWEEN 45.7645 AND 45.7748
       AND position.lng BETWEEN 4.8131 AND 4.8398 THEN 'Lyon1'
      WHEN position.lat BETWEEN 45.7711 AND 45.7897
       AND position.lng BETWEEN 4.8090 AND 4.8430 THEN 'Lyon4'
      WHEN position.lat BETWEEN 45.7636 AND 45.7871
       AND position.lng BETWEEN 4.8394 AND 4.8699 THEN 'Lyon6'
      WHEN position.lat BETWEEN 45.7390 AND 45.7637
       AND position.lng BETWEEN 4.8384 AND 4.8984 THEN 'Lyon3'
      WHEN position.lat BETWEEN 45.7442 AND 45.7680
       AND position.lng BETWEEN 4.7718 AND 4.8305 THEN 'Lyon5'
      WHEN position.lat BETWEEN 45.7188 AND 45.7494
       AND position.lng BETWEEN 4.8476 AND 4.8921 THEN 'Lyon8'
      WHEN position.lat BETWEEN 45.7074 AND 45.7568
       AND position.lng BETWEEN 4.8187 AND 4.8600 THEN 'Lyon7'
      WHEN position.lat BETWEEN 45.7587 AND 45.8083
       AND position.lng BETWEEN 4.7840 AND 4.8412 THEN 'Lyon9'
      WHEN position.lat BETWEEN 45.7265 AND 45.7663
       AND position.lng BETWEEN 4.8130 AND 4.8398 THEN 'Lyon2'
      ELSE 'Hors_Lyon'
    END AS quartier,
    ROW_NUMBER() OVER (PARTITION BY number ORDER BY last_update DESC) AS rn
  FROM raw_velo_stations
  WHERE bike_stands > 0  -- exclut les stations à capacité nulle (anomalie)
)
SELECT
  quartier,
  AVG(available_bikes / bike_stands) AS taux_remplissage_moyen,
  AVG(available_bike_stands) AS places_libres_moyennes
FROM stations_courantes
WHERE rn = 1
GROUP BY quartier
HAVING AVG(available_bikes / bike_stands) > 0.85
   AND AVG(available_bike_stands) < 15
ORDER BY taux_remplissage_moyen DESC;

-- ============================================================================
-- Q6 — Quelle proportion des stations dispose de coordonnées GPS et d'un
--      statut valides ?
--
-- Critères de validité :
--   - position.lat et position.lng non nuls et dans la bbox du Grand Lyon
--   - status non nul et égal à OPEN ou CLOSED (valeurs reconnues par JCDecaux)
--
-- On agrège sur l'ensemble des observations (toutes les snapshots), pas
-- uniquement sur le dernier — la question porte sur la qualité globale du parc.
-- ============================================================================

SELECT
  COUNT(*) AS total_observations,
  SUM(CASE
    WHEN position.lat IS NOT NULL
     AND position.lng IS NOT NULL
     AND position.lat BETWEEN 45.65 AND 45.85
     AND position.lng BETWEEN 4.75 AND 4.95
     AND status IN ('OPEN', 'CLOSED')
    THEN 1 ELSE 0
  END) AS observations_valides,
  100.0 * SUM(CASE
    WHEN position.lat IS NOT NULL
     AND position.lng IS NOT NULL
     AND position.lat BETWEEN 45.65 AND 45.85
     AND position.lng BETWEEN 4.75 AND 4.95
     AND status IN ('OPEN', 'CLOSED')
    THEN 1 ELSE 0
  END) / COUNT(*) AS proportion_valides_pourcent
FROM raw_velo_stations;
