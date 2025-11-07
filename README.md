<h1 align="center">🧩 HadoopTests</h1>

<p align="center">
  <strong>Mini écosystème Hadoop complet pour développements, tests, intégrations et apprentissage</strong><br>
  <em>Basé sur la librairie hadoop-minicluster</em>
</p>

---

## 🎯 Objectif

Le projet **HadoopTests** permet de recréer en local un environnement **Hadoop complet** pour exécuter des tests unitaires, d’intégration et fonctionnels en conditions réelles, sans infrastructure lourde.  
Il s’appuie sur la librairie **hadoop-minicluster** et fournit plusieurs modules pour illustrer différents usages.

---

## 🧱 Structure du projet

| Module | Description |
|--------|--------------|
| 🧩 **minicluster-core** | Cœur du système : gestion du cycle de vie du MiniCluster (HDFS, YARN, Hive, Spark, HBase, etc.) |
| 🚀 **app** | Exemple d’application client consommant le cluster (ingestion HDFS, requêtes Hive et Spark) |
| 🧪 **app-test** | Jeux de tests unitaires et d’intégration couvrant les composants Hadoop |

---

## ⚙️ Prérequis

- ☕ **JDK 8**  
- 🧰 **Maven 3.8+**  
- 💻 Linux / macOS / Windows

---

## 🚀 Démarrage rapide

```bash
# Cloner le repo
git clone https://github.com/smiloudi-jee/HadoopTests.git
cd HadoopTests
