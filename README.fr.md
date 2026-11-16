# CityFlow — Real-Time Traffic Intelligence

**Projet 3 — Écosystème Big Data 2**  
Prototype de traitement de flux en temps réel avec **Apache Spark Structured Streaming**.

> 🇬🇧 English version: [`README.md`](README.md)

| | |
|---|---|
| **Module** | Écosystème Big Data 2 |
| **Enseignante** | Pr. Soukaina Gouraguine |
| **Année universitaire** | 2026–2027 |
| **Réalisé par** | Mazine CHATER · Yahya BAID · Wenn Bouka DIEUMERCI |
| **Scénario** | Trafic urbain de Casablanca, 8 zones, 2 heures de pointe |
| **Stack** | Python 3.12 · PySpark 3.5.3 · Jupyter · Parquet |

---

## 1. Objectif

Une régie de mobilité collecte en continu les passages de véhicules sur des capteurs
répartis dans huit zones de Casablanca. Le prototype doit produire, au fil de l'eau,
des indicateurs fiables par **zone** et par **fenêtre de 10 minutes** :

- volume de passages (`vehicle_count`) ;
- vitesses moyenne, médiane et 95e percentile ;
- taux de congestion et état de trafic ;
- nombre de capteurs actifs.

La difficulté principale n'est pas le calcul : c'est de le faire **au bon moment**, sur le
**bon intervalle de temps**, alors que certains événements arrivent en retard. CityFlow
démontre :

1. l'ingestion d'un flux JSON avec un schéma explicite ;
2. le nettoyage et l'enrichissement par jointure **stream-static** ;
3. l'agrégation **event-time** avec fenêtres tumblantes et **watermark** ;
4. l'écriture dans des sinks durables et checkpointés (Bronze / Gold) ;
5. la réconciliation des volumes et la comparaison batch / streaming.

---

## 2. Livrables

| Livrable | Contenu | Emplacement |
|---|---|---|
| **L1 — Présentation** | 23 slides de soutenance, notes orateur, PDF | `L1_Presentation/` |
| **L2 — Code** | Notebook exécuté + scripts Python reproductibles | `L2_Code/` |
| **L3 — Dataset** | 12 fichiers JSON, référentiel zones, métadonnées | `L3_Dataset/` |
| **L4 — Fiche de TP** | Énoncé pas à pas, manipulations, résultats attendus | `L4_Fiche_TP/` |
| **Résultats** | Parquet, CSV, JSON et figures | `output/` |

---

## 3. Structure du projet

```text
Projet3_CityFlow_RealTime_Traffic/
├── L1_Presentation/
│   ├── CityFlow_Presentation.pptx     # support de soutenance
│   ├── CityFlow_Presentation.pdf      # export PDF
│   ├── build_presentation.py          # génération du support
│   └── assets/                        # figures et crops
├── L2_Code/
│   ├── notebooks/
│   │   └── CityFlow_Streaming.ipynb   # livrable pédagogique exécuté
│   ├── scripts/
│   │   ├── generate_dataset.py        # générateur déterministe
│   │   ├── inspect_dataset.py         # contrôle rapide du dataset
│   │   ├── spark_session.py           # session Spark + Hadoop Windows
│   │   ├── cityflow_lib.py            # schéma, nettoyage, agrégations, sinks
│   │   ├── cityflow_pipeline.py       # CLI : available / live / batch
│   │   ├── stream_producer.py         # arrivée progressive des fichiers
│   │   └── build_figures.py           # figures et tableaux de synthèse
│   └── requirements.txt
├── L3_Dataset/
│   ├── data/
│   │   ├── stream/                    # 12 fichiers JSON Lines
│   │   ├── reference/zones.csv        # référentiel des 8 zones
│   │   └── metadata.json              # volumétrie et paramètres
│   └── README_dataset.md
├── L4_Fiche_TP/
│   ├── Fiche_TP_CityFlow.md
│   ├── Fiche_TP_CityFlow.docx
│   └── Fiche_TP_CityFlow.pdf
├── output/
│   ├── bronze_events/                 # événements enrichis
│   ├── gold_windows/                  # agrégats finalisés
│   ├── batch_windows/                 # référence batch
│   ├── metrics/                       # métriques par micro-batch
│   ├── tables/                        # CSV et KPI pour la soutenance
│   ├── results_summary.json
│   └── batch_summary.json
├── tools/hadoop/bin/                  # winutils.exe + hadoop.dll (Windows)
└── README.md
```

---

## 4. Prérequis

- **Python 3.12** (3.10+ recommandé) ;
- **Java 17** (Temurin ou OpenJDK), avec `JAVA_HOME` défini ;
- **PySpark 3.5.3** ;
- environ 4 Go de RAM disponibles ;
- Windows, Linux ou macOS.

Sous Windows, le projet embarque `winutils.exe` et `hadoop.dll` dans
`tools/hadoop/bin/`. `spark_session.py` les copie automatiquement dans
`%TEMP%\cityflow_hadoop\bin` (chemin sans espace ni caractère non ASCII) et configure
`HADOOP_HOME` ainsi que `java.library.path`. Aucune variable d'environnement Hadoop
n'est donc à créer manuellement.

### Installation

```powershell
cd "Projet3_CityFlow_RealTime_Traffic"
python -m pip install --upgrade pip
python -m pip install -r L2_Code/requirements.txt
```

Vérification rapide :

```powershell
python -c "import pyspark, pandas, matplotlib; print(pyspark.__version__)"
java -version
```

---

## 5. Démarrage rapide

### 5.1 Le dataset est déjà fourni

Les 12 fichiers JSON, le référentiel `zones.csv` et `metadata.json` sont présents dans
`L3_Dataset/data/`. Il n'est pas nécessaire de les régénérer pour démontrer le projet.

Contrôle rapide de l'intégrité du dataset :

```powershell
python L2_Code/scripts/inspect_dataset.py
```

Le script vérifie le nombre de fichiers, de lignes, de lignes corrompues et de retards,
puis affiche `Statut : OK`.

### 5.2 Exécuter le pipeline en mode reproductible

Le mode `available` traite tous les fichiers présents, **un fichier par micro-batch**,
puis s'arrête. C'est le mode utilisé pour les mesures et la soutenance.

```powershell
python L2_Code/scripts/cityflow_pipeline.py --mode available
```

Sorties :

- `output/bronze_events/` : événements valides, typés et enrichis (Parquet) ;
- `output/gold_windows/` : agrégats zone × fenêtre finalisés (Parquet) ;
- `output/metrics/stream_batches.jsonl` : métriques de chaque micro-batch ;
- `output/results_summary.json` : synthèse et KPI.

### 5.3 Comparer avec le mode batch

```powershell
python L2_Code/scripts/cityflow_pipeline.py --mode batch
```

Le mode batch lit les mêmes fichiers en une passe statique et écrit
`output/batch_windows/` ainsi que `output/batch_summary.json`.

### 5.4 Ouvrir le notebook

```powershell
jupyter notebook L2_Code/notebooks/CityFlow_Streaming.ipynb
```

Le notebook est déjà exécuté (sorties et graphiques inclus). Pour le rejouer de bout en
bout :

```powershell
jupyter nbconvert --to notebook --execute --inplace `
  L2_Code/notebooks/CityFlow_Streaming.ipynb
```

### 5.5 Simuler un flux continu (mode live)

Deux terminaux sont nécessaires.

**Terminal 1 — producteur de fichiers :**

```powershell
python L2_Code/scripts/stream_producer.py --clean --interval 5
```

**Terminal 2 — pipeline en écoute :**

```powershell
python L2_Code/scripts/cityflow_pipeline.py `
  --mode live `
  --source-dir L3_Dataset/data/incoming `
  --duration 120
```

Le producteur copie les 12 fichiers un par un dans `L3_Dataset/data/incoming/`.
Le pipeline les détecte et met à jour ses agrégats. `Ctrl+C` arrête proprement les
requêtes.

### 5.6 Régénérer le dataset (optionnel)

```powershell
python L2_Code/scripts/generate_dataset.py --clean
```

Le générateur est **déterministe** (graine `20261102`) : les événements produits sont
identiques d'une exécution à l'autre. Seul l'horodatage `generated_at` des métadonnées
change.

### 5.7 Reconstruire les figures et la présentation (optionnel)

```powershell
python L2_Code/scripts/build_figures.py
python L1_Presentation/build_presentation.py
```

La présentation nécessite `python-pptx`. L'export PDF peut être produit avec
LibreOffice :

```powershell
& "C:\Program Files\LibreOffice\program\soffice.com" `
  --headless --convert-to pdf `
  --outdir L1_Presentation L1_Presentation/CityFlow_Presentation.pptx
```

> **Important (Windows + OneDrive).** Si le chemin du projet contient des caractères
> non ASCII, LibreOffice peut échouer lors de l'écriture directe dans le dossier.
> Convertir d'abord vers un chemin neutre, par exemple
> `C:\Users\<user>\AppData\Local\Temp\cityflow_pdf`, puis copier le PDF.

---

## 6. Résultats de référence

Mesures obtenues sur le poste de développement (Python 3.12, PySpark 3.5.3, Java 17).
Les durées varient selon la machine ; les volumes, eux, sont déterministes.

| Indicateur | Valeur |
|---|---:|
| Fichiers source | 12 |
| Lignes générées | 52 846 |
| Lignes corrompues | 216 (0,4 %) |
| Événements valides | 52 630 (99,6 %) |
| Événements en retard | 2 194 (4,2 %) |
| Agrégats zone × fenêtre (batch) | 96 |
| Agrégats finalisés en streaming | 80 |
| Événements dans Gold | 43 710 (83,1 %) |
| Événements rejetés par le watermark | 198 (0,4 %) |
| Événements dans les fenêtres ouvertes | 8 722 (16,6 %) |
| Micro-batchs | 13 |
| Durée streaming | 86,27 s |
| Durée batch complète | 25,09 s |
| Latence moyenne par micro-batch | 6,88 s |
| Débit moyen | 610 événements/s |
| Zone la plus chargée | Centre-Ville (8 377 passages) |
| Congestion maximale | Centre-Ville (87,3 %) |
| Pic de congestion | 08:10 (97,2 %) |
| Pic de volume (Gold) | 07:20 (4 505 passages, toutes zones) |

Le pic de volume ci-dessus porte sur les **fenêtres finalisées** (Gold). Le calcul batch
complet, qui inclut les 198 événements tardifs et les fenêtres encore ouvertes, culmine à
**4 555 passages à 07:00**.

**Équation de cohérence :**

```text
52 630 événements valides
= 43 710 finalisés dans Gold
+    198 rejetés par le watermark
+  8 722 encore dans les fenêtres ouvertes
```

Les deux dernières fenêtres (`08:40–08:50` et `08:50–09:00`) restent ouvertes parce que
le flux est fini : aucun événement postérieur à 09:00 ne vient faire avancer le watermark.
C'est le comportement attendu, et non une perte de données.

---

## 7. Architecture du pipeline

```text
Fichiers JSON (arrivée progressive)
        │
        ▼
readStream + schéma explicite + maxFilesPerTrigger=1
        │
        ▼
Nettoyage / typage / validation métier
        │
        ▼
Jointure broadcast avec zones.csv (stream-static)
        │
        ├──► Bronze Parquet : événements enrichis (audit, rejeu)
        │
        ▼
withWatermark(event_time, "10 minutes")
groupBy(window(event_time, "10 minutes"), zone_id)
        │
        ├──► Gold Parquet : agrégats zone × fenêtre finalisés
        ├──► Sink mémoire : vue SQL de supervision
        └──► Console : contrôle des fenêtres globales
```

Choix structurants :

- **Schéma explicite** : pas d'inférence coûteuse, lignes invalides maîtrisées ;
- **`maxFilesPerTrigger=1`** : un micro-batch par fichier, comportement observable ;
- **Watermark 10 min** : borne les retards acceptés et libère l'état ;
- **Jointure broadcast** : le référentiel zones est petit, aucun shuffle inutile ;
- **`foreachBatch`** : ajout du `batch_id` et journalisation des métriques ;
- **Checkpoints séparés** : chaque requête reprend indépendamment.

---

## 8. Choix techniques et pièges corrigés

| Sujet | Décision |
|---|---|
| Source | Fichiers JSON Lines, sans Kafka (non imposé par le cahier des charges) |
| Agrégation | `window(event_time, "10 minutes")`, fenêtre tumblante |
| Retards | `withWatermark("event_time", "10 minutes")` |
| Comptage distinct | `approx_count_distinct` (compatible streaming) |
| Nom de fichier source | `input_file_name()` appliqué **avant** la jointure statique |
| Sink mémoire | `outputMode("complete")` pour éviter les doublons |
| `foreachBatch` | `batch_df.count()` plutôt que `rdd.isEmpty()` |
| Windows | `winutils.exe` / `hadoop.dll` copiés dans `%TEMP%` |

Ces points sont détaillés dans le notebook et dans la présentation (slide 13).

---

## 9. Dataset

Le dataset est **entièrement synthétique** : aucune donnée personnelle, aucun capteur
réel. Il est conçu pour être exigeant :

- 12 fichiers de 10 minutes, du 2 novembre 2026, 07:00 à 09:00 UTC ;
- 8 zones de Casablanca, 26 capteurs, 5 types de véhicules ;
- 4,2 % d'événements retardés (6 à 45 minutes) ;
- 0,4 % de lignes JSON volontairement corrompues.

La description complète (origine, schéma, volumétrie, limites) se trouve dans
[`L3_Dataset/README_dataset.md`](L3_Dataset/README_dataset.md).

---

## 10. Limites et perspectives

**Limites assumées :**

- la source fichier simule un flux mais ne remplace pas un broker de production ;
- 12 micro-lots ne permettent pas de mesurer un débit à grande échelle ;
- le watermark est fixe (10 min), non adaptatif ;
- pas de déduplication sur `event_id` ;
- Parquet n'offre ni transactions ACID ni évolution de schéma ;
- pas de tableau de bord ni d'alerte en production.

**Perspectives :**

- remplacer les fichiers par **Apache Kafka** ;
- passer à **Delta Lake / Iceberg** pour l'ACID et le time travel ;
- dédupliquer avec `event_id` et un state store ;
- rendre le watermark adaptatif à la latence observée ;
- brancher **Grafana / Superset** et des alertes sur seuils de congestion.

---

## 11. Dépannage

| Symptôme | Cause probable | Solution |
|---|---|---|
| `winutils.exe` introuvable | binaires Hadoop absents | vérifier `tools/hadoop/bin/winutils.exe` |
| `Python worker failed to connect` | interpreter différent | définir `PYSPARK_PYTHON` sur le même Python |
| `JAVA_HOME is not set` | JDK absent | installer Java 17 et définir `JAVA_HOME` |
| Checkpoint invalide | ancien état conservé | supprimer `output/checkpoints/` et relancer |
| Fichiers non détectés en live | producteur non lancé | lancer `stream_producer.py` dans un second terminal |
| Conversion PDF en erreur | chemin OneDrive non ASCII | convertir vers `%TEMP%` puis copier |
| Notebook lent au premier lancement | démarrage JVM / Spark | attendre la fin de l'initialisation |

---

## 12. Reproductibilité

- graine du générateur : `20261102` ;
- période : `2026-11-02T07:00:00Z` → `2026-11-02T09:00:00Z` ;
- fuseau : UTC ;
- code source : `L2_Code/scripts/` ;
- sorties de référence : `output/` ;
- notebook exécuté : `L2_Code/notebooks/CityFlow_Streaming.ipynb` ;
- présentation : `L1_Presentation/CityFlow_Presentation.pptx` et `.pdf` ;
- fiche de TP : `L4_Fiche_TP/Fiche_TP_CityFlow.pdf`.

Pour rejouer l'ensemble :

```powershell
python L2_Code/scripts/generate_dataset.py --clean
python L2_Code/scripts/cityflow_pipeline.py --mode available
python L2_Code/scripts/cityflow_pipeline.py --mode batch
python L2_Code/scripts/build_figures.py
python L1_Presentation/build_presentation.py
```

---

*Projet réalisé dans le cadre du module Écosystème Big Data 2 — 2026–2027.*
