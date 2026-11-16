# Dataset CityFlow — Real-Time Traffic Intelligence

**Livrable L3 — jeu de données du projet**  
Module Écosystème Big Data 2 · Projet 3 · 2026–2027  
Mazine CHATER · Yahya BAID · Wenn Bouka DIEUMERCI

---

## 1. Origine des données

Le dataset est **entièrement synthétique**. Il ne provient d'aucun capteur réel et ne
contient aucune donnée personnelle. Il a été produit par le script
[`L2_Code/scripts/generate_dataset.py`](../L2_Code/scripts/generate_dataset.py) pour
simuler le flux d'une régie de mobilité urbaine à Casablanca.

Le scénario retenu est une **matinée de pointe**, avec un profil de congestion qui monte
de 07:00 à 08:10 puis décroît progressivement jusqu'à 09:00. Les volumes, vitesses,
retards et lignes corrompues sont générés de façon **déterministe** à partir de la graine
`20261102`.

| Paramètre | Valeur |
|---|---|
| Générateur | `generate_dataset.py` (bibliothèque standard Python) |
| Graine | `20261102` |
| Période | `2026-11-02T07:00:00Z` → `2026-11-02T09:00:00Z` |
| Fuseau | UTC |
| Fichiers | 12 fichiers JSON Lines, un par tranche de 10 minutes |
| Zones | 8 zones de Casablanca |
| Capteurs | 26 capteurs (`Z01-S01` … `Z08-S04`) |
| Types de véhicules | `car`, `taxi`, `moto`, `bus`, `truck` |
| Taille totale | environ 10,8 Mo |

Le choix d'un générateur déterministe permet de rejouer exactement les mêmes agrégats,
de comparer batch et streaming sur les mêmes données et de vérifier les résultats
attendus de la fiche de TP.

---

## 2. Contenu du dossier

```text
L3_Dataset/
└── data/
    ├── stream/
    │   ├── traffic_20261102_0700_0710.json
    │   ├── traffic_20261102_0710_0720.json
    │   ├── ...
    │   └── traffic_20261102_0850_0900.json
    ├── reference/
    │   └── zones.csv
    └── metadata.json
```

### 2.1 `data/stream/*.json`

Chaque fichier représente **10 minutes de temps d'événement**. Il contient un objet JSON
par ligne (JSON Lines), encodé en UTF-8. L'ordre des lignes n'est pas chronologique :
c'est volontaire, afin de reproduire l'arrivée désordonnée d'un flux.

Exemple de ligne valide :

```json
{"event_id": "EVT-202611020700-000001", "event_time": "2026-11-02T07:00:22.630Z", "zone_id": "Z01", "zone_name": "Centre-Ville", "sensor_id": "Z01-S01", "lane": 1, "vehicle_type": "taxi", "speed_kmh": 24.8}
```

Exemple de ligne corrompue (JSON tronqué, injecté volontairement) :

```json
{"event_id": "EVT-BROKEN", "event_time": "2026-11-02T07:0
```

### 2.2 `data/reference/zones.csv`

Référentiel statique des huit zones. Il est joint au flux par une **jointure
stream-static broadcast**.

| Colonne | Type | Description |
|---|---|---|
| `zone_id` | string | Identifiant de zone (`Z01` … `Z08`) |
| `zone_name` | string | Nom lisible de la zone |
| `district` | string | Arrondissement de Casablanca |
| `speed_limit_kmh` | int | Limite légale de vitesse |
| `lanes` | int | Nombre de voies du tronçon |

Contenu :

| zone_id | zone_name | district | speed_limit_kmh | lanes |
|---|---|---|---:|---:|
| Z01 | Centre-Ville | Casablanca - Sidi Belyout | 40 | 4 |
| Z02 | Maarif | Casablanca - Maarif | 40 | 3 |
| Z03 | Ain Diab | Casablanca - Ain Diab | 60 | 3 |
| Z04 | Sidi Maarouf | Casablanca - Sidi Maarouf | 60 | 4 |
| Z05 | Hay Hassani | Casablanca - Hay Hassani | 50 | 3 |
| Z06 | Anfa | Casablanca - Anfa | 40 | 2 |
| Z07 | Bourgogne | Casablanca - Anfa | 50 | 3 |
| Z08 | Ain Sebaa | Casablanca - Ain Sebaa | 60 | 4 |

### 2.3 `data/metadata.json`

Métadonnées de génération : graine, période, nombre de fichiers, volumétrie, nombre de
retards, nombre de lignes corrompues et statistiques par fichier. Ce fichier est la
référence pour vérifier que le dataset n'a pas été modifié.

---

## 3. Schéma du flux

Le schéma est imposé explicitement dans `cityflow_lib.EVENT_SCHEMA`. Il ne dépend pas de
l'inférence de Spark.

| Colonne | Type Spark | Description | Contraintes |
|---|---|---|---|
| `event_id` | `string` | Identifiant du passage | non nul, unique dans le dataset |
| `event_time` | `string` → `timestamp` | Horodatage de l'événement (UTC, ISO-8601) | non nul, convertible |
| `zone_id` | `string` | Zone géographique | format `^Z\d{2}$` |
| `zone_name` | `string` | Nom de la zone | information redondante, retirée avant la jointure |
| `sensor_id` | `string` | Capteur à l'origine de la mesure | format `Zxx-Syy` |
| `lane` | `int` | Voie de circulation | 1 à `lanes` |
| `vehicle_type` | `string` | Type de véhicule | `car`, `taxi`, `moto`, `bus`, `truck` |
| `speed_kmh` | `double` | Vitesse instantanée mesurée | entre 0 et 200 km/h |

> **Remarque.** `zone_name` est présent dans le flux pour rendre les fichiers lisibles,
> mais le pipeline le supprime (`drop("zone_name")`) avant la jointure avec `zones.csv`.
> Cela évite une colonne dupliquée et garantit que le nom de zone vient du référentiel.

---

## 4. Volumétrie et qualité

Statistiques issues de `metadata.json` :

| Indicateur | Valeur | Part |
|---|---:|---:|
| Lignes générées | 52 846 | 100 % |
| Lignes JSON corrompues | 216 | 0,4 % |
| Événements valides après nettoyage | 52 630 | 99,6 % |
| Événements retardés | 2 194 | 4,2 % |
| Fichiers | 12 | — |
| Zones | 8 | — |
| Capteurs | 26 | — |

### 4.1 Retards

Environ 4,2 % des événements sont livrés avec un retard de **6 à 45 minutes**. Le retard
est simulé en modifiant l'`event_time` de l'événement : il est inséré dans le fichier
courant alors que sa fenêtre temporelle est déjà ancienne. Comme le watermark est fixé à
10 minutes, les retards supérieurs à cette tolérance sont rejetés par l'agrégation
stateful.

Sur le flux complet :

- **2 194** événements sont arrivés en retard ;
- **198** ont été rejetés par le watermark ;
- les autres ont été acceptés parce que leur fenêtre était encore ouverte.

### 4.2 Lignes corrompues

Environ 0,4 % des lignes sont des JSON tronqués. En mode `PERMISSIVE`, Spark les convertit
en lignes de valeurs nulles ; le nettoyage les filtre via `event_id IS NOT NULL` et
`event_time IS NOT NULL`. Elles ne doivent jamais faire échouer le job.

### 4.3 Distribution des vitesses

Le générateur applique :

- une limite de vitesse par zone ;
- un facteur de fluidité propre à la zone ;
- un profil horaire de congestion (plus fort vers 08:00–08:30) ;
- une pénalité selon le type de véhicule (les bus et camions roulent moins vite) ;
- un bruit gaussien borné.

Les vitesses sont plafonnées à `1,25 × speed_limit_kmh` et plancher à 5 km/h. Le taux de
congestion d'un événement est calculé dans le pipeline : `speed_kmh < 0,60 × speed_limit`.

---

## 5. Reproduction du dataset

```powershell
cd "Projet3_CityFlow_RealTime_Traffic"
python L2_Code/scripts/generate_dataset.py --clean
```

Options utiles :

```powershell
python L2_Code/scripts/generate_dataset.py --files 6
python L2_Code/scripts/generate_dataset.py --seed 42
```

- `--clean` supprime `data/stream/` avant de régénérer ;
- `--files N` produit seulement les N premiers micro-lots ;
- `--seed` change la graine (les résultats ne seront plus ceux de la fiche de TP).

Le script réécrit également `reference/zones.csv` et `metadata.json`.

### Vérification rapide

Compter les lignes et les fichiers avec Python :

```python
from pathlib import Path
import json

root = Path("L3_Dataset/data")
files = sorted((root / "stream").glob("*.json"))
lines = sum(1 for f in files for _ in f.open(encoding="utf-8"))
meta = json.loads((root / "metadata.json").read_text(encoding="utf-8"))

print(len(files), "fichiers")
print(lines, "lignes")
print(meta["total_events"], "événements attendus")
print(meta["late_events"], "retards")
```

Résultat attendu avec la graine `20261102` :

```text
12 fichiers
52846 lignes
52846 événements attendus
2194 retards
```

Le script [`inspect_dataset.py`](../L2_Code/scripts/inspect_dataset.py) automatise ce
contrôle et affiche `Statut : OK` lorsque les volumétries correspondent aux métadonnées.

---

## 6. Utilisation avec Spark

### Lecture batch (contrôle)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("CityFlowCheck").getOrCreate()
df = spark.read.json("L3_Dataset/data/stream")
print(df.count())
df.printSchema()
```

### Lecture streaming

```python
stream = (
    spark.readStream
    .schema("event_id string, event_time string, zone_id string, zone_name string, "
            "sensor_id string, lane int, vehicle_type string, speed_kmh double")
    .option("maxFilesPerTrigger", 1)
    .json("L3_Dataset/data/stream")
)
```

Le pipeline complet, avec nettoyage, jointure et watermark, se trouve dans
`L2_Code/scripts/cityflow_lib.py` et `cityflow_pipeline.py`.

---

## 7. Limites et précautions

- **Données simulées** : le dataset ne reflète pas la réalité opérationnelle de la
  circulation à Casablanca. Il ne doit pas servir à une décision de terrain.
- **Topologie simplifiée** : 8 zones, 26 capteurs, aucune information GPS ni météo.
- **Deux heures seulement** : le volume reste petit (≈ 53 000 lignes) et ne permet pas
  un benchmark à grande échelle.
- **Retards et corruptions injectés** : ils sont volontairement exagérés pour rendre les
  mécanismes de watermark et de tolérance aux erreurs observables.
- **Horodatage UTC** : aucun changement d'heure ni fuseau local n'est modélisé.
- **`generated_at`** : ce champ change à chaque régénération, même si les événements sont
  identiques. Les comparaisons doivent porter sur les données, pas sur ce champ.
- **Pas de déduplication** : `event_id` est unique dans le dataset, mais le pipeline ne
  déduplique pas les réémissions. Cette limite est assumée et documentée.

---

## 8. Licence et usage

Dataset produit dans un cadre **pédagogique** pour le module Écosystème Big Data 2
(2026–2027). Réutilisation libre pour des travaux d'enseignement, avec mention du projet
CityFlow et des auteurs. Aucune donnée personnelle, aucun secret, aucune donnée
propriétaire n'est incluse.

---

*Pour toute question sur la génération ou le schéma, se référer au notebook
`L2_Code/notebooks/CityFlow_Streaming.ipynb` et à la fiche de TP
`L4_Fiche_TP/Fiche_TP_CityFlow.pdf`.*
