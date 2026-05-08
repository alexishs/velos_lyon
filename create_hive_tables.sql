-- Tables Hive EXTERNAL pointant sur les datasets HDFS du data lake.
-- Trois datasets, conformément aux livrables de l'énoncé :
--   - raw_velo_stations    → couche raw (snapshots JCDecaux ingérés depuis Kafka)
--   - processed_load_factor → couche processed (sortie MR1, agrégats par station)
--   - curated_quartiers     → couche curated (sortie MR3, agrégats par heure × arrondissement)
--
-- Exécution :
--   beeline -u jdbc:hive2://hive-server:10000 -f create_hive_tables.sql
-- ou depuis le conteneur dev (pyhive) :
--   python -c "from pyhive import hive; ..."

-- ============================================================================
-- 1. raw_velo_stations : snapshots bruts par station
-- ============================================================================

-- Le SerDe JSON natif de Hive (org.apache.hive.hcatalog.data.JsonSerDe) est embarqué
-- dans hive-hcatalog-core. Il est ajouté ici pour éviter une erreur de classpath
-- si le jar n'est pas chargé par défaut.
ADD JAR /opt/hive/hcatalog/share/hcatalog/hive-hcatalog-core-2.3.2.jar;

DROP TABLE IF EXISTS raw_velo_stations;
CREATE EXTERNAL TABLE raw_velo_stations (
  number INT,
  contract_name STRING,
  name STRING,
  address STRING,
  position STRUCT<lat:DOUBLE, lng:DOUBLE>,
  banking BOOLEAN,
  bonus BOOLEAN,
  bike_stands INT,
  available_bike_stands INT,
  available_bikes INT,
  status STRING,
  last_update BIGINT
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/data-lake/raw/velo_lyon/';

-- Activation de la lecture récursive : les snapshots JSONL sont rangés dans des
-- sous-répertoires par heure (/data-lake/raw/velo_lyon/YYYY-MM-DD-HH/...).
-- Ces SET doivent être en début de session avant tout SELECT sur la table.
SET hive.mapred.supports.subdirectories=true;
SET mapreduce.input.fileinputformat.input.dir.recursive=true;

-- ============================================================================
-- 2. processed_load_factor : sortie MR1 (un enregistrement par station)
-- ============================================================================

DROP TABLE IF EXISTS processed_load_factor;
CREATE EXTERNAL TABLE processed_load_factor (
  station_id INT,
  avg_load_factor DOUBLE,
  std_load DOUBLE,
  nb_samples_valides INT,
  total_samples INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data-lake/processed/load_metrics/';

-- ============================================================================
-- 3. curated_quartiers : sortie MR3 (agrégats par heure UTC × arrondissement)
-- ============================================================================

DROP TABLE IF EXISTS curated_quartiers;
CREATE EXTERNAL TABLE curated_quartiers (
  heure STRING,
  quartier STRING,
  p95_load DOUBLE,
  nb_stations INT,
  capacite_totale INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data-lake/processed/horaire/';

-- ============================================================================
-- Vérification : compte de lignes par table après création
-- ============================================================================

SELECT 'raw_velo_stations' AS dataset, COUNT(*) AS nb_lignes FROM raw_velo_stations
UNION ALL
SELECT 'processed_load_factor', COUNT(*) FROM processed_load_factor
UNION ALL
SELECT 'curated_quartiers', COUNT(*) FROM curated_quartiers;
