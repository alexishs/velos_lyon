# velos_lyon

> **Avertissement** : cet environnement est destiné au développement local uniquement. Ne pas utiliser en production (mots de passe en clair, ports exposés sans authentification, pas de haute disponibilité).

## Démarrage

```bash
# Renseigner votre UID/GID (nécessaire pour Airflow)
echo "AIRFLOW_UID=$(id -u)" >> .env
echo "AIRFLOW_GID=$(id -g)" >> .env

# Lancer les services
docker compose up -d
```

Les DAGs Airflow sont à placer dans le répertoire `dag/` à la racine du projet.

## URLs disponibles

### Interfaces web

| Service | URL | Description |
|---|---|---|
| **HDFS NameNode** | http://localhost:9870 | Interface web du système de fichiers distribué HDFS |
| **YARN ResourceManager** | http://localhost:8088 | Interface de gestion des ressources et des jobs MapReduce |
| **YARN History Server** | http://localhost:8188 | Historique des jobs MapReduce terminés |
| **Apache Atlas** | http://localhost:21000 | Gouvernance des données et catalogage des métadonnées |
| **Airflow** | http://localhost:8080 | Orchestration des pipelines (admin / admin) |

### Connexions non-HTTP (clients programmatiques)

| Service | Adresse | Description |
|---|---|---|
| **HDFS** | `hdfs://localhost:9000` | Accès au système de fichiers |
| **Hive Metastore** | `localhost:9083` | Thrift endpoint pour Hive |
| **HiveServer2** | `localhost:10000` | Connexion JDBC/ODBC à Hive |
| **Kafka** | `localhost:9092` | Broker Kafka (KRaft mode) |
| **PostgreSQL Airflow** | `localhost:5432` | Base de données Airflow (user: `airflow`, password: `airflow`, db: `airflow`) |