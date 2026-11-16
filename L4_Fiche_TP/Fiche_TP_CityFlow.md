# Fiche de TP — CityFlow : traitement de flux en temps réel

**Module :** Écosystème Big Data 2 — Pr. Soukaina Gouraguine  
**Projet 3 :** City Flow — Real-Time Traffic Intelligence  
**Année universitaire :** 2026–2027  
**Auteurs du prototype :** Mazine CHATER · Yahya BAID · Wenn Bouka DIEUMERCI

---

## 1. Informations générales

| Élément | Contenu |
|---|---|
| **Public** | 5e année — module Écosystème Big Data 2 |
| **Durée** | 3 h (dont 30 min d'installation et de prise en main) |
| **Modalité** | Binôme ou trinôme, sur poste individuel |
| **Outils** | Python 3.12, PySpark 3.5.3, Jupyter, Parquet |
| **Prérequis** | Python, notions de DataFrame, SQL de base, terminal |
| **Livrables attendus** | Notebook exécuté, réponses aux questions, court rapport (4 à 6 pages) |
| **Barème** | 20 points (détail en section 12) |

### 1.1 Objectifs pédagogiques

À l'issue de ce TP, l'étudiant doit être capable de :

1. distinguer **traitement batch** et **traitement de flux**, et expliquer la notion de
   **table infinie** ;
2. distinguer **temps de l'événement** (`event_time`) et **temps de traitement**
   (`processing_time`) ;
3. construire un pipeline **Spark Structured Streaming** avec schéma explicite,
   nettoyage, enrichissement et agrégation ;
4. mettre en œuvre une **fenêtre tumblante de 10 minutes** et un **watermark** ;
5. écrire dans des sinks **Parquet** et **mémoire** avec checkpoints ;
6. réconcilier les volumes et interpréter les résultats métier ;
7. comparer les performances et les garanties du batch et du streaming ;
8. diagnostiquer les pièges classiques du streaming.

### 1.2 Problématique

> Comment calculer en continu, et de façon fiable, des indicateurs de trafic par zone et
> par fenêtre de 10 minutes, à partir d'un flux d'événements qui arrive progressivement et
> dont certains événements sont retardés ?

Le TP répond à cette question sur un jeu de données synthétique représentant **deux heures
de trafic à Casablanca**, avec huit zones, 26 capteurs, des retards et des lignes JSON
corrompues.

<!-- pagebreak -->

## 2. Environnement de travail

### 2.1 Prérequis techniques

| Composant | Version conseillée | Vérification |
|---|---|---|
| Python | 3.12 (3.10+) | `python --version` |
| Java | 17 (Temurin ou OpenJDK) | `java -version` |
| PySpark | 3.5.3 | `python -c "import pyspark; print(pyspark.__version__)"` |
| Jupyter | JupyterLab 4 ou Notebook 7 | `jupyter --version` |
| RAM | 4 Go minimum | — |

Sous Windows, `JAVA_HOME` doit pointer vers le JDK 17. Le projet fournit
`winutils.exe` et `hadoop.dll` dans `tools/hadoop/bin/` ; le script
`spark_session.py` les copie automatiquement dans `%TEMP%\cityflow_hadoop\bin` pour
éviter les problèmes de chemin Unicode.

### 2.2 Installation

Ouvrir un terminal à la racine du projet, puis :

```powershell
cd "Projet3_CityFlow_RealTime_Traffic"
python -m pip install --upgrade pip
python -m pip install -r L2_Code/requirements.txt
```

Contrôle rapide :

```powershell
python -c "import pyspark, pandas, matplotlib; print('OK', pyspark.__version__)"
java -version
```

### 2.3 Arborescence utile

```text
Projet3_CityFlow_RealTime_Traffic/
├── L1_Presentation/          # support de soutenance
├── L2_Code/
│   ├── notebooks/CityFlow_Streaming.ipynb
│   └── scripts/
│       ├── generate_dataset.py
│       ├── cityflow_lib.py
│       ├── cityflow_pipeline.py
│       ├── stream_producer.py
│       └── build_figures.py
├── L3_Dataset/data/
│   ├── stream/               # 12 fichiers JSON
│   ├── reference/zones.csv
│   └── metadata.json
├── output/                   # résultats à analyser
└── tools/hadoop/bin/         # binaires Windows
```

> **À retenir.** Les scripts doivent être lancés depuis la racine du projet. Les chemins
> sont résolus à partir de `Path(__file__).resolve().parents[2]`.

<!-- pagebreak -->

## 3. Le jeu de données

Le dataset est **entièrement synthétique** et **déterministe** : la graine `20261102`
garantit les mêmes événements à chaque génération. Il ne contient aucune donnée
personnelle.

| Indicateur | Valeur |
|---|---:|
| Fichiers JSON | 12 |
| Période | 2 novembre 2026, 07:00 → 09:00 UTC |
| Zones | 8 |
| Capteurs | 26 |
| Lignes générées | 52 846 |
| Lignes corrompues | 216 (0,4 %) |
| Événements valides | 52 630 (99,6 %) |
| Événements retardés | 2 194 (4,2 %) |

### 3.1 Schéma du flux

| Colonne | Type | Rôle |
|---|---|---|
| `event_id` | string | identifiant du passage |
| `event_time` | string ISO-8601 → timestamp | moment réel de l'événement (UTC) |
| `zone_id` | string | zone (`Z01` … `Z08`) |
| `zone_name` | string | nom de la zone (redondant, retiré avant la jointure) |
| `sensor_id` | string | capteur (`Z01-S01` …) |
| `lane` | int | voie de circulation |
| `vehicle_type` | string | `car`, `taxi`, `moto`, `bus`, `truck` |
| `speed_kmh` | double | vitesse instantanée |

Le référentiel `zones.csv` apporte `district`, `speed_limit_kmh` et `lanes`. Il est joint
au flux par une **jointure stream-static broadcast**.

### 3.2 Pourquoi des données corrompues et retardées ?

- Les **216 lignes JSON tronquées** vérifient que le pipeline ne s'arrête pas sur une
  erreur de parsing : en mode `PERMISSIVE`, elles deviennent des lignes de `NULL`, puis
  sont filtrées.
- Les **2 194 événements retardés** (6 à 45 minutes) vérifient que le watermark accepte
  les retards dans sa tolérance et rejette proprement les autres.

Ces défauts sont **volontaires**. Le pipeline doit les traiter, pas les ignorer.

<!-- pagebreak -->

## 4. Étape 1 — Inspecter le dataset (20 min)

### 4.1 Vérifier les métadonnées et la volumétrie

```powershell
python L2_Code/scripts/inspect_dataset.py
```

Résultat attendu :

```text
Contrôle du dataset CityFlow
========================================
Graine                : 20261102
Période               : 2026-11-02T07:00:00Z -> 2026-11-02T09:00:00Z
Fichiers              : 12
Lignes comptées       : 52846
Lignes JSON valides   : 52630
Lignes JSON invalides : 216
Événements attendus   : 52846
Événements retardés   : 2194 (4.2%)
Zones                 : 8
========================================
Statut                : OK
```

Le script vérifie **trois choses** : le nombre de fichiers, le nombre total de lignes et
le nombre de lignes JSON invalides. Le statut `OK` confirme que le dataset est cohérent
avec `metadata.json`.

### 4.2 Lire quelques lignes brutes

```powershell
Get-Content L3_Dataset\data\stream\traffic_20261102_0700_0710.json -TotalCount 3
```

Observer :

- le format **JSON Lines** : un objet par ligne, pas de tableau global ;
- l'`event_time` en UTC avec le suffixe `Z` ;
- l'ordre des lignes, qui n'est pas chronologique ;
- la présence éventuelle de lignes tronquées.

### 4.3 Questions de contrôle

1. Pourquoi le nombre de lignes générées (52 846) diffère-t-il du nombre d'événements
   valides (52 630) ?
2. Pourquoi l'ordre des lignes dans un fichier n'est-il pas chronologique ?
3. Combien de capteurs distincts le dataset contient-il ? Comment le vérifier ?
4. Pourquoi le dataset contient-il volontairement des erreurs ?

> **Points de contrôle.** 216 lignes sont corrompues ; les retards sont insérés dans le
> fichier courant avec un `event_time` antérieur ; il y a 26 capteurs ; les erreurs
> permettent de tester la robustesse du pipeline.

<!-- pagebreak -->

## 5. Étape 2 — Établir la référence batch (25 min)

Le mode batch calcule les agrégats sur **toutes les données déjà présentes**, sans
watermark ni état incrémental. Il sert de référence de comparaison.

### 5.1 Lancer le batch

```powershell
python L2_Code/scripts/cityflow_pipeline.py --mode batch
```

Sortie attendue (les durées varient selon la machine) :

```text
Événements traités : 52 630
Fenêtres calculées : 96
Durée batch        : 25,09 s
Résultats          : .../output/batch_windows
```

### 5.2 Vérifier le résultat

```powershell
Get-Content output\batch_summary.json
```

Le fichier contient :

```json
{
  "mode": "batch",
  "elapsed_seconds": 25.09,
  "compute_seconds": 16.32,
  "write_seconds": 8.76,
  "valid_events": 52630,
  "windows": 96
}
```

### 5.3 Interpréter le nombre de fenêtres

Il y a **12 fenêtres de 10 minutes** et **8 zones** :

```text
12 × 8 = 96 agrégats zone × fenêtre
```

Le batch produit bien les 96 agrégats, y compris les deux dernières fenêtres
(`08:40–08:50` et `08:50–09:00`), puisque rien ne l'empêche de lire la fin du fichier.

### 5.4 Questions de contrôle

1. Pourquoi le batch produit-il 96 agrégats alors que le streaming n'en finalise que 80 ?
2. Le batch gère-t-il les événements retardés ? Que deviennent-ils ?
3. Le batch utilise-t-il un checkpoint ? Pourquoi ?
4. Quel est l'inconvénient principal du batch dans un contexte de supervision ?

> **Points de contrôle.** Le batch lit tout, sans watermark ; il intègre donc les 198
> événements que le streaming rejette. Il n'a pas besoin de checkpoint car il repart de
> zéro à chaque exécution. Son inconvénient est le délai : il attend la fin de la collecte.

<!-- pagebreak -->

## 6. Étape 3 — Exécuter le pipeline streaming (35 min)

### 6.1 Lancer le mode `available`

```powershell
python L2_Code/scripts/cityflow_pipeline.py --mode available
```

Ce mode traite tous les fichiers présents, **un fichier par micro-batch**
(`maxFilesPerTrigger=1`), puis s'arrête (`trigger(availableNow=True)`). Il est
reproductible et adapté à la démonstration.

### 6.2 Lire la synthèse

```powershell
Get-Content output\results_summary.json
```

Sortie console attendue :

```text
Fichiers sources             : 12
Micro-batchs exécutés        : 13
Événements valides (Bronze)  : 52 630
Fenêtres finalisées (Gold)   : 80
Événements dans les fenêtres : 43 710
Événements tardifs ignorés   : 198
Événements en fenêtre ouverte: 8 722
Fenêtres (calcul batch)      : 96
Durée totale streaming       : 86,27 s
```

> **Pourquoi 13 micro-batchs ?** 12 fichiers produisent 12 micro-batchs de données, et
> Spark exécute un dernier lot de clôture sans nouvelles lignes. Ce lot final permet de
> publier les fenêtres devenues finalisables.

### 6.3 Vérifier les sorties

| Sortie | Contenu | Vérification |
|---|---|---|
| `output/bronze_events/` | événements valides typés et enrichis | 52 630 lignes |
| `output/gold_windows/` | agrégats zone × fenêtre finalisés | 80 lignes |
| `output/metrics/stream_batches.jsonl` | métriques par micro-batch | 13 lignes |
| `output/checkpoints/` | offsets et état des requêtes | dossiers non vides |

Compter les lignes Gold avec Spark :

```powershell
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[*]').appName('check').getOrCreate(); print(s.read.parquet('output/gold_windows').count()); s.stop()"
```

Résultat attendu : `80`.

### 6.4 Questions de contrôle

1. Pourquoi 80 agrégats finalisés et non 96 ?
2. Pourquoi 8 722 événements ne sont-ils ni dans Gold ni rejetés ?
3. Pourquoi 198 événements sont-ils rejetés ? Est-ce une perte de données ?
4. À quoi servent les checkpoints ? Que se passerait-il sans eux ?
5. Pourquoi le premier micro-batch est-il plus lent que les suivants ?

> **Points de contrôle.** Les deux dernières fenêtres restent ouvertes car le flux est
> fini ; le watermark ne peut pas les dépasser sans événement postérieur à 09:00. Les 198
> rejets sont des retards au-delà de la tolérance, comptabilisés. Les checkpoints
> permettent de reprendre sans relire tout le flux. Le premier lot paie le démarrage de
> la JVM et du State Store.

<!-- pagebreak -->

## 7. Étape 4 — Analyser les résultats Gold (30 min)

Ouvrir le notebook :

```powershell
jupyter notebook L2_Code/notebooks/CityFlow_Streaming.ipynb
```

Exécuter les cellules de la section **7. Résultats et interprétation**, ou utiliser les
requêtes ci-dessous dans un notebook Spark.

### 7.1 Top des zones

```python
from pyspark.sql import functions as F

gold = spark.read.parquet("output/gold_windows")

(gold.groupBy("zone_id", "zone_name")
     .agg(F.sum("vehicle_count").alias("passages"),
          F.round(F.avg("avg_speed_kmh"), 1).alias("vitesse_moyenne"),
          F.round(F.avg("congestion_rate"), 3).alias("congestion"))
     .orderBy(F.desc("passages"))
     .show(8, truncate=False))
```

Résultat attendu :

| Zone | Passages | Vitesse moyenne | Congestion |
|---|---:|---:|---:|
| Centre-Ville | 8 377 | 17,7 km/h | 87,3 % |
| Maarif | 7 793 | 18,9 km/h | 83,5 % |
| Sidi Maarouf | 7 160 | 39,9 km/h | 44,8 % |
| Hay Hassani | 6 322 | 26,4 km/h | 75,2 % |
| Ain Sebaa | 6 212 | 36,6 km/h | 57,5 % |

### 7.2 Pic de congestion

```python
(gold.groupBy("window_start")
     .agg(F.round(F.avg("congestion_rate"), 3).alias("congestion_moyenne"),
          F.sum("vehicle_count").alias("passages"))
     .orderBy(F.desc("congestion_moyenne"))
     .show(5, truncate=False))
```

Résultat attendu : le pic de congestion moyenne est la fenêtre **08:10**, avec
**97,2 %** de congestion moyenne.

### 7.3 Pic de volume

```python
(gold.groupBy("window_start")
     .agg(F.sum("vehicle_count").alias("passages"))
     .orderBy(F.desc("passages"))
     .show(5, truncate=False))
```

Résultat attendu : dans Gold, le pic est **07:20**, avec **4 505 passages** toutes zones ;
le batch complet (96 fenêtres) culmine à 4 555 à 07:00.

### 7.4 États de trafic

```python
(gold.groupBy("traffic_state")
     .count()
     .orderBy(F.desc("count"))
     .show())
```

Observer la répartition entre `Critique`, `Dense`, `Fluide` et `Libre`.

### 7.5 Questions de contrôle

1. Le pic de congestion coïncide-t-il avec le pic de volume ? Pourquoi ?
2. Quelles zones sont structurellement congestionnées ? Quelles zones restent fluides ?
3. Pourquoi la vitesse moyenne est-elle un meilleur indicateur que le volume seul ?
4. Comment le taux de congestion est-il calculé ? Quel seuil est utilisé ?
5. Pourquoi le pic de volume dans Gold diffère-t-il légèrement du batch complet ?

> **Points de contrôle.** Le pic de congestion (08:10) ne coïncide pas avec le pic de
> volume (07:20) : la congestion dépend de la vitesse, pas seulement du nombre de
> véhicules. Le taux de congestion est la part des passages sous 60 % de la limite de
> vitesse. Les 198 rejets et les deux fenêtres ouvertes expliquent l'écart de volume.

<!-- pagebreak -->

## 8. Étape 5 — Expérience watermark (25 min)

Cette expérience construit un flux minimal pour observer un rejet **certain**.

### 8.1 Principe

| Fichier | Événement | Rôle |
|---|---|---|
| `f1.json` | 10:05 | ouvre la fenêtre 10:00–10:10 |
| `f2.json` | 10:15 | avance le watermark |
| `f3.json` | 10:25 | ferme la fenêtre 10:00–10:10 |
| `f4.json` | 10:35 + **09:30** | l'événement de 09:30 doit être ignoré |

L'événement de 09:30 appartient à une fenêtre déjà finalisée. Il doit être comptabilisé
dans `numRowsDroppedByWatermark`, sans apparaître dans les agrégats.

### 8.2 Exécution

Dans le notebook, exécuter la section **8. Expérience contrôlée : données tardives et
watermark** (cellules 34 à 36). Le notebook crée les quatre fichiers dans
`L2_Code/notebooks/_runtime/watermark_demo/`, lance un `readStream` et affiche le
résultat.

### 8.3 Résultat attendu

```text
Fenêtres observées : 10:00–10:10, 10:10–10:20, 10:20–10:30, 10:30–10:40
Événement 09:30    : absent des agrégats
numRowsDroppedByWatermark = 1
```

### 8.4 Questions de contrôle

1. Pourquoi l'événement de 09:30 est-il rejeté alors que celui de 10:05 est accepté ?
2. Que se passerait-il sans watermark ?
3. Pourquoi le watermark ne peut-il pas être infiniment grand ?
4. Quel réglage choisiriez-vous si le réseau de capteurs a une latence de 20 minutes ?

> **Points de contrôle.** Le watermark vaut `max(event_time) − 10 min`. À 10:25, il est à
> 10:15 : la fenêtre 09:30–09:40 est close. Sans watermark, l'état grossirait
> indéfiniment et des fenêtres anciennes pourraient être modifiées. Un watermark plus grand
> accepte plus de retards mais coûte en mémoire et en latence.

<!-- pagebreak -->

## 9. Étape 6 — Mode live et reprise (20 min, optionnel)

Le mode `live` traite un répertoire alimenté progressivement. Il illustre le fonctionnement
continu et les checkpoints.

### 9.1 Lancer deux terminaux

**Terminal 1 — producteur :**

```powershell
python L2_Code/scripts/stream_producer.py --clean --interval 5
```

**Terminal 2 — pipeline :**

```powershell
python L2_Code/scripts/cityflow_pipeline.py --mode live --source-dir L3_Dataset/data/incoming --duration 120
```

### 9.2 Observer

- le producteur copie un fichier toutes les 5 secondes ;
- le pipeline détecte chaque nouveau fichier ;
- la console affiche les fenêtres au fil de l'eau ;
- les sorties vont dans `output/live_bronze/` et `output/live_gold/`.

### 9.3 Tester la reprise

1. Arrêter le pipeline avec `Ctrl+C` après quelques fichiers.
2. Relancer la même commande **sans supprimer** `output/live_checkpoints/`.
3. Observer que Spark reprend à l'offset enregistré et ne relit pas les fichiers déjà
   traités.

### 9.4 Questions de contrôle

1. Quelle différence entre `available` et `live` ?
2. Pourquoi les checkpoints sont-ils séparés par requête ?
3. Que se passe-t-il si l'on supprime le checkpoint puis que l'on relance ?
4. Pourquoi le mode live n'est-il pas utilisé pour les mesures de référence ?

> **Points de contrôle.** `available` traite l'existant puis s'arrête ; `live` attend de
> nouveaux fichiers. Chaque requête a son propre offset et son propre état. Supprimer le
> checkpoint provoque une relecture complète. Les mesures de référence utilisent
> `available` pour être reproductibles.

<!-- pagebreak -->

## 10. Étape 7 — Comparer batch et streaming (20 min)

### 10.1 Tableau de comparaison à compléter

À partir de `output/batch_summary.json` et `output/results_summary.json`, compléter :

| Critère | Batch | Streaming |
|---|---|---|
| Nombre d'agrégats produits | 96 | 80 finalisés + 16 ouverts |
| Premier résultat | après toute la collecte | ~4 s après le premier fichier |
| Durée totale | 25,09 s | 86,27 s (dont démarrage) |
| Gestion des retards | aucune gestion spécifique | watermark, 198 rejets |
| Reprise | relance complète | checkpoint |
| Usage | audit, rejeu, historique | supervision, alerte, tableau de bord |

### 10.2 Interprétation attendue

Le batch est plus rapide **quand toutes les données sont déjà disponibles** et plus simple
pour le rejeu. Le streaming produit un résultat **plus tôt**, gère explicitement les
retards et reprend sur checkpoint. En production, les deux coexistent souvent : le
streaming alimente le tableau de bord, le batch sert à l'historique et à l'audit.

### 10.3 Questions de contrôle

1. Le streaming est-il « plus rapide » que le batch ? Justifier.
2. Pourquoi les durées ne sont-elles pas directement comparables ?
3. Dans quel cas choisiriez-vous uniquement le batch ?
4. Dans quel cas le streaming est-il indispensable ?

<!-- pagebreak -->

## 11. Résultats attendus et vérifications

### 11.1 Tableau de référence

| Indicateur | Valeur attendue |
|---|---:|
| Fichiers traités | 12 |
| Lignes générées | 52 846 |
| Lignes corrompues | 216 |
| Événements valides (Bronze) | 52 630 |
| Événements retardés | 2 194 |
| Agrégats zone × fenêtre (batch) | 96 |
| Agrégats finalisés (Gold) | 80 |
| Événements dans Gold | 43 710 |
| Rejetés par le watermark | 198 |
| Événements en fenêtres ouvertes | 8 722 |
| Micro-batchs | 13 |
| Durée streaming | ≈ 86 s |
| Durée batch | ≈ 25 s |
| Latence moyenne par micro-batch | ≈ 6,9 s |
| Débit moyen | ≈ 610 événements/s |
| Zone la plus chargée | Centre-Ville (8 377) |
| Congestion maximale | Centre-Ville (87,3 %) |
| Pic de congestion | 08:10 (97,2 %) |
| Pic de volume dans Gold | 07:20 (4 505) |

### 11.2 Équation de cohérence

```text
52 630 événements valides
= 43 710 finalisés dans Gold
+    198 rejetés par le watermark
+  8 722 encore dans les fenêtres ouvertes
```

Cette équation est le **test de correction principal** : aucun événement ne doit être
perdu silencieusement.

### 11.3 Checklist de validation

- [ ] `metadata.json` contient `seed = 20261102` et `total_events = 52846`.
- [ ] Le mode batch se termine sans erreur et produit 96 fenêtres.
- [ ] Le mode `available` se termine sans erreur.
- [ ] Bronze contient 52 630 événements.
- [ ] Gold contient 80 agrégats.
- [ ] `results_summary.json` indique 198 rejets et 8 722 événements ouverts.
- [ ] L'équation de cohérence est vérifiée à l'unité près.
- [ ] Les deux dernières fenêtres sont absentes de Gold.
- [ ] Les checkpoints sont présents et non vides.
- [ ] L'expérience watermark rejette exactement 1 événement.
- [ ] Le notebook s'exécute de bout en bout sans erreur.
- [ ] Les graphiques et tableaux sont cohérents avec les KPI.

### 11.4 Erreurs fréquentes

| Symptôme | Cause | Correction |
|---|---|---|
| `winutils.exe` introuvable | binaires Hadoop absents | vérifier `tools/hadoop/bin/` |
| `Python worker failed to connect` | mauvais interpréteur | définir `PYSPARK_PYTHON` |
| `JAVA_HOME is not set` | JDK absent | installer Java 17 |
| Checkpoint invalide | ancien état | supprimer `output/checkpoints/` |
| `countDistinct` refusé | non supporté en streaming | utiliser `approx_count_distinct` |
| `input_file_name()` échoue après jointure | évalué sur deux sources | l'appeler avant la jointure |
| Sink mémoire avec doublons | mode `update` | utiliser `outputMode("complete")` |
| `rdd.isEmpty()` bloque | action non supportée | utiliser `batch_df.count()` |

<!-- pagebreak -->

## 12. Travail à rendre et barème

### 12.1 Rapport attendu (4 à 6 pages)

1. **Page de garde** : noms, groupe, date.
2. **Objectifs et démarche** : ce qui a été compris, pas seulement les commandes.
3. **Résultats** : tableau de référence, équation de cohérence, captures ou extraits.
4. **Analyse métier** : zones congestionnées, pics, interprétation.
5. **Expérience watermark** : protocole, observation, conclusion.
6. **Comparaison batch / streaming** : tableau et justification.
7. **Difficultés rencontrées** et solutions.
8. **Conclusion** : limites et perspectives.

Le notebook exécuté et les réponses aux questions peuvent être joints en annexe.

### 12.2 Barème (20 points)

| Critère | Points |
|---|---:|
| Installation, exécution et reproductibilité | 4 |
| Compréhension des concepts (event time, fenêtre, watermark) | 4 |
| Exactitude des résultats et équation de cohérence | 4 |
| Analyse métier et interprétation | 3 |
| Expérience watermark | 2 |
| Comparaison batch / streaming | 2 |
| Qualité du rapport et des réponses | 1 |

**Bonus (jusqu'à 2 points) :** test de robustesse supplémentaire, visualisation
personnelle pertinente, ou proposition d'industrialisation argumentée.

### 12.3 Critères d'évaluation

- les chiffres doivent être **reproductibles** et cohérents avec le dataset ;
- les réponses doivent expliquer le **pourquoi**, pas seulement le **combien** ;
- les captures d'écran doivent être lisibles et accompagnées d'une interprétation ;
- toute divergence doit être **expliquée**, pas masquée.

<!-- pagebreak -->

## 13. Annexes

### Annexe A — Commandes essentielles

```powershell
# Installation
python -m pip install -r L2_Code/requirements.txt

# Dataset (optionnel)
python L2_Code/scripts/generate_dataset.py --clean

# Pipeline
python L2_Code/scripts/cityflow_pipeline.py --mode available
python L2_Code/scripts/cityflow_pipeline.py --mode batch

# Notebook
jupyter notebook L2_Code/notebooks/CityFlow_Streaming.ipynb

# Mode live (deux terminaux)
python L2_Code/scripts/stream_producer.py --clean --interval 5
python L2_Code/scripts/cityflow_pipeline.py --mode live --source-dir L3_Dataset/data/incoming --duration 120

# Figures et présentation
python L2_Code/scripts/build_figures.py
python L1_Presentation/build_presentation.py
```

### Annexe B — Requêtes SQL utiles

```sql
-- Top 5 des zones par volume
SELECT zone_name, SUM(vehicle_count) AS passages
FROM gold
GROUP BY zone_name
ORDER BY passages DESC
LIMIT 5;

-- Fenêtres les plus congestionnées
SELECT window_start, ROUND(AVG(congestion_rate), 3) AS congestion
FROM gold
GROUP BY window_start
ORDER BY congestion DESC
LIMIT 5;

-- Répartition des états de trafic
SELECT traffic_state, COUNT(*) AS nb
FROM gold
GROUP BY traffic_state
ORDER BY nb DESC;
```

### Annexe C — Structure d'un agrégat Gold

```text
zone_id, zone_name, district, speed_limit_kmh
vehicle_count, avg_speed_kmh, median_speed_kmh, p95_speed_kmh
min_speed_kmh, max_speed_kmh, congested_events, active_sensors
window_start, window_end, avg_speed_ratio, congestion_rate
traffic_state, computed_at, batch_id
```

### Annexe D — Pour aller plus loin

- remplacer la source fichier par **Kafka** ;
- écrire dans **Delta Lake** pour l'ACID et le time travel ;
- dédupliquer sur `event_id` avec un state store ;
- rendre le watermark adaptatif ;
- brancher **Grafana / Superset** et des alertes.

---

Fiche de TP CityFlow — Écosystème Big Data 2 — 2026–2027.
