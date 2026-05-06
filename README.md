# Data Lake Vélo Lyon

Énoncé du TP : [ENONCE.md](ENONCE.md)

> **Avertissement** : cet environnement est destiné au développement local uniquement. Ne pas utiliser en production (mots de passe en clair, ports exposés sans authentification, pas de haute disponibilité).

## Démarrage

```bash
# Renseigner votre UID (nécessaire pour Airflow et le conteneur de dev)
echo "AIRFLOW_UID=$(id -u)" >> .env

# Lancer les services
docker compose up -d
```

Les DAGs Airflow sont à placer dans le répertoire `dag/` à la racine du projet.

## Environnement de développement

Un conteneur `dev` a été ajouté à la stack. Il embarque Python 3.12, Airflow 2.10.5 et debugpy, et est monté sur le répertoire racine du projet.

Les images Hadoop (`bde2020`) et Hive ne disposent pas d'environnement Python exploitable pour le développement. Écrire et déboguer les scripts depuis la machine locale poserait un problème de résolution DNS : les services de la stack (`namenode`, `kafka`, `postgres-airflow`…) ne sont accessibles que depuis l'intérieur du réseau Docker `data-net`.

Le conteneur `dev` résout les deux problèmes : il est sur `data-net` et peut donc joindre tous les services par leur nom, tout en offrant un environnement Python complet. VS Code s'y connecte via l'extension Dev Containers (`Ctrl+Shift+P` → *Reopen in Container*), ce qui permet d'éditer, exécuter et déboguer les scripts avec des breakpoints.

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
| **HDFS** | `hdfs://localhost:9000` | Accès au filesystem |
| **Hive Metastore** | `localhost:9083` | Thrift endpoint pour Hive |
| **HiveServer2** | `localhost:10000` | Connexion JDBC/ODBC à Hive |
| **Kafka** | `localhost:9092` | Broker Kafka (KRaft mode) |
| **PostgreSQL Airflow** | `localhost:5432` | Base de données Airflow (user: `airflow`, password: `airflow`, db: `airflow`) |
