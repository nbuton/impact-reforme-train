# Source de données
- data/communes-france-2025.csv : https://www.data.gouv.fr/datasets/communes-et-villes-de-france-en-csv-excel-json-parquet-et-feather   
- data/gestionnaires-rrn-2025.csv : https://www.data.gouv.fr/datasets/gestionnaires-du-reseau-routier-national   

# 📊 Méthodologie du comparateur de mobilité

Ce projet propose un comparateur de prix entre le train (actuel vs réformé), le bus et la voiture, basé sur des données réelles de distances et de péages.

## 🛠️ Fonctionnement du Calcul

Le système repose sur une architecture en deux étapes : une phase de pré-calcul des données géographiques et une phase de simulation tarifaire dynamique.

### 1. Pré-calcul des données (Python)
Le script `pre_compute_distances_and_peage_price.py` génère le socle de données (`.csv`) en interrogeant des sources officielles :

* **Distance Train :** Extraite via l'**API SNCF** (`api.sncf.com`). Le script calcule la distance réelle sur rails en additionnant la longueur de chaque section de transport public du meilleur itinéraire trouvé.
* **Distance Voiture :** Calculée via l'**API OSRM** (Open Source Routing Machine) pour obtenir l'itinéraire routier le plus rapide.
* **Estimation des Péages :** * Le script utilise les données du **Réseau Routier National** pour identifier les sections d'autoroutes concédées (payantes).
    * Il effectue un croisement géographique (avec un buffer de 1000m) entre l'itinéraire OSRM et les segments payants.
    * Le coût est estimé sur la base de **0,13 €/km** sur les sections identifiées comme concédées.

### 2. Simulation Tarifaire (JavaScript)
Le site web applique ensuite des formules dynamiques basées sur les distances récupérées :

| Mode de transport | Base de calcul | Hypothèses |
| :--- | :--- | :--- |
| **Train Actuel** | Distance Rail | 12 € / 100 km |
| **Train Réformé** | Distance Rail | **-49,5%** par rapport au tarif actuel |
| **Bus** | Distance Route | 6,70 € / 100 km |
| **Voiture** | Distance Route | Conso: 4,7L/100km \| Carburant: 1,75€/L |

**Calcul du coût voiture :**
Le coût affiché est le `(Coût Carburant + Péage total) / Nombre de passagers`.

## 📂 Structure des fichiers
* `pre_compute_distances_and_peage_price.py` : Script de génération des données.
* `index.html` : Interface du comparateur et moteur de calcul JS.
* `explanation.html` : Détails pédagogiques sur les enjeux de la réforme.

---
*Ce comparateur est un outil de simulation basé sur des moyennes nationales et des données d'infrastructures réelles.*
# Config html
```js
const CONFIG = {
    CSV_URL: 'https://nbuton.github.io/impact-reforme-train/comparatif.csv',
    CAR_CONSUMPTION_L_PER_100KM: 4.7,
    FUEL_PRICE_EUR_PER_L: 1.75,
    TRAIN_PRICE_EUR_PER_100KM: 12,
    BUS_PRICE_EUR_PER_100KM: 6.7,
    TRAIN_REFORM_DISCOUNT: 0.495 // 49,5% de réduction
};
```