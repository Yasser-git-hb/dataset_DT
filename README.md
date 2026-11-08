# Digital Twin – Simulation thermique d'une maison (Rabat)

## Description

Ce projet génère un **dataset de simulation thermique horaire** d'une maison à Rabat (Maroc) en utilisant **EnergyPlus**. Le script `generer_dataset_horaire.py` fait varier systématiquement :

- **8 orientations** du bâtiment (0°, 45°, 90°, …, 315°)
- **4 fenêtres** (ouvertes ou fermées) dans les zones Z2 à Z5
- **4 portes** (ouvertes ou fermées) dans les zones Z2 à Z5

Cela produit **2 048 scénarios** (8 × 2⁸ combinaisons). Pour chaque scénario, EnergyPlus simule une année complète (8 760 heures) et enregistre par zone :

| Variable | Description |
|---|---|
| `z<n>_T` | Température de l'air (°C) |
| `z<n>_HR` | Humidité relative (%) |
| `z<n>_TRM` | Température radiante moyenne (°C) |

Le modèle utilise **AirflowNetwork** (ventilation naturelle multi-zone) ajouté dynamiquement en mémoire. Le fichier IDF source (`maisonfinal.idf`) n'est jamais modifié.

## Fichiers principaux

| Fichier | Rôle |
|---|---|
| `generer_dataset_horaire.py` | Script principal de génération du dataset |
| `maisonfinal.idf` | Modèle EnergyPlus de la maison |
| `MAR_RK_Rabat-Sale.AP.601350_TMYx.2009-2023.epw` | Fichier météo annuel typique (TMY) de Rabat |

## Résultats (sous-dossier `dataset_horaire/`)

Les scénarios générés se trouvent dans le sous-dossier **`dataset_horaire/`** :

```
dataset_horaire/
├── scenarios.csv               # Table de tous les scénarios (id, angle, états des ouvertures)
├── meteo_horaire.parquet       # Météo extérieure horaire (commune à tous les scénarios)
├── maison_afn_exemple.idf      # IDF avec AirflowNetwork (pour inspection manuelle)
├── horaire/                    # Un fichier Parquet par scénario
│   ├── S0001.parquet           #   8 760 lignes × colonnes thermiques par zone
│   ├── S0002.parquet
│   ├── ...
│   └── S2048.parquet
├── erreurs/                    # Fichiers .err et .idf des simulations échouées
└── travail/                    # Dossier temporaire (nettoyé automatiquement)
```

## Utilisation

```bash
# Test rapide (4 simulations)
python generer_dataset_horaire.py --smoke

# Lancer toutes les simulations (reprend automatiquement là où ça s'est arrêté)
python generer_dataset_horaire.py

# Lancer des scénarios spécifiques
python generer_dataset_horaire.py --ids 6,8

# Options utiles
python generer_dataset_horaire.py --workers 4        # nombre de simulations en parallèle
python generer_dataset_horaire.py --pas 2             # 2 pas de calcul par heure
python generer_dataset_horaire.py --garder-eclairage  # ne pas retirer l'éclairage du modèle
```

**Ctrl+C** pour interrompre proprement ; relancer la même commande pour reprendre.

## Prérequis

- **Python 3** avec `numpy`, `pandas`, `pyarrow` (pour le format Parquet)
- **EnergyPlus** installé (détecté automatiquement sous `C:\EnergyPlus*`, ou spécifier avec `--ep CHEMIN`)

