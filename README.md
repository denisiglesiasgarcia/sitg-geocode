# sitg-geocode

Client Python pour corriger et géocoder des adresses via l'API SITG Lab (Genève).

API : <https://geocodage.sitg-lab.ch>

## Installation

```bash
# Installer uv https://docs.astral.sh/uv/getting-started/installation/

# Mac/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

```bash
# Depuis le dépôt git
uv add git+https://github.com/denisiglesiasgarcia/sitg-geocode

# Créer venv
uv sync
```

## Usage

### Inspecter la réponse brute de l'API

```python
from sitg_geocode import inspect_sitg_response

await inspect_sitg_response("Av. J-D Maillard, 7, Meyrin")
```

```json
{
  "hits": [
    {
      "IDPADR": "930225017479",
      "ADRESSE": "Avenue Jacob-Daniel-MAILLARD 7",
      "TYPE": "Existante",
      "TYVOIE": "Avenue",
      "TYPABR": "av.",
      "LIANT": "Jacob-Daniel-",
      "NOMVOI": "MAILLARD",
      "NO_ADRESSE": "7",
      "COMMUNE": "Meyrin",
      "NO_POSTAL": "1217",
      "NOM_NPA": "Meyrin",
      "EGID": "1021280",
      "EGRID": "CH276384676537",
      "longitude": 6.068229,
      "latitude": 46.22923,
      "easting": 2494278.54,
      "northing": 1120679.36,
      "indexed_at": "2026-05-08T06:02:03.508686+00:00",
      "score": 78.43
    }
  ],
  "offset": 0,
  "limit": 1,
  "nbHits": 1,
  "processingTimeMs": 1,
  "query": "maillard"
}
```

### Géocoder une liste d'adresses

```python
import polars as pl
from sitg_geocode import sitg_geocode_async

df = pl.read_csv("test_adresses.csv")

result_geocode = await sitg_geocode_async(
    df,
    col_adresse="Rue et N°",
    output_format="polars",
    max_concurrent=10,
    min_score_threshold=95,
)

# Joindre les résultats au DataFrame original
df_resultat = df.join(result_geocode, on="Rue et N°", how="left")
df_resultat.write_csv("resultat.csv")

print(df_resultat.head(5))
```

Résultat: 

| Rue et N° | SITG_ADRESSE | SITG_NPA | SITG_NOM_NPA | SITG_COMMUNE | SITG_EGID | SITG_EGRID | SITG_SCORE | SITG_LON | SITG_LAT | SITG_EST_EPSG_2056 | SITG_NORD_EPSG_2056 |
|-----------|--------------|----------|--------------|--------------|-----------|------------|-----------|----------|----------|-----------------|-----------------|
| "12 rue Jean-Charles Amat" | "Rue Jean-Charles-AMAT 12" | 1202 | "Genève" | "Genève-Petit-Saconnex" | 2037721 | "CH168165638919" | 95.83 | 6.148414 | 46.21442 | 2.5004e6 | 1.1189e6 |
| "37, sous Garan" | null | null | null | null | null | null | null | null | null | null | null |
| "Ancienne route, 78" | null | null | null | null | null | null | null | null | null | null | null |
| "Av de Thonex 30" | "Avenue de Thônex 30" | 1225 | "Chêne-Bourg" | "Chêne-Bourg" | 295511715 | "CH576391476529" | 100.0 | 6.198379 | 46.19087 | 2.5043e6 | 1.1162e6 |
| "Av. Blanc, 5" | null | null | null | null | null | null | null | null | null | null | null |

## Paramètres de `sitg_geocode_async`

| Paramètre              | Type              | Défaut      | Description                                                                 |
|------------------------|-------------------|-------------|-----------------------------------------------------------------------------|
| `df`                   | `pl.DataFrame` \| `pd.DataFrame` | — | DataFrame en entrée (Polars ou Pandas)                    |
| `col_adresse`          | `str`             | —           | Nom de la colonne contenant les adresses à géocoder                         |
| `output_format`        | `str`             | `"polars"`  | Format de sortie : `"polars"` ou `"pandas"`                                 |
| `max_concurrent`       | `int`             | `10`        | Nombre maximum de requêtes HTTP simultanées                                 |
| `min_score_threshold`  | `float`           | `0.0`       | Score minimum pour conserver un résultat (0–100). `0.0` = conserver tout   |

## Colonnes retournées

Le DataFrame résultat de l'exemple `df_resultat` contient la colonne d'adresse originale (clé de jointure) et les champs suivants :

| Colonne                | Type       | Description                                          |
|------------------------|------------|------------------------------------------------------|
| `SITG_ADRESSE`         | `String`   | Adresse normalisée par le SITG                       |
| `SITG_NPA`             | `Int64`    | Code postal                                          |
| `SITG_NOM_NPA`         | `String`   | Nom de la localité                                   |
| `SITG_COMMUNE`         | `String`   | Commune                                              |
| `SITG_EGID`            | `Int64`    | EGID - Identifiant fédéral du bâtiment (RegBL)       |
| `SITG_EGRID`           | `String`   | EGRID - Identifiant fédéral de l'immeuble (RF)       |
| `SITG_SCORE`           | `Float64`  | Score de confiance du géocodage (0–100)              |
| `SITG_LON`             | `Float64`  | Longitude WGS84                                      |
| `SITG_LAT`             | `Float64`  | Latitude WGS84                                       |
| `SITG_EST_EPSG_2056`   | `Float64`  | Coordonnée Est LV95 / EPSG:2056                      |
| `SITG_NORD_EPSG_2056`  | `Float64`  | Coordonnée Nord LV95 / EPSG:2056                     |

> Toutes les colonnes SITG sont nullables : si le géocodage échoue pour une adresse, les champs correspondants sont `null`.

## Validation du schéma

La fonction `validate_schema` permet de vérifier que le DataFrame respecte le schéma attendu. Elle est appelée automatiquement après chaque géocodage et logue les écarts éventuels. Elle peut aussi être utilisée manuellement :

```python
from sitg_geocode import validate_schema

errors = validate_schema(result_geocode)
if errors:
    for err in errors:
        print(err)
```

## Notes

- Le géocodage est asynchrone : utiliser `await` ou `asyncio.run()` selon le contexte.
- Le paramètre `min_score_threshold` filtre les lignes dont le score est inférieur au seuil ; les adresses non retrouvées (score `null`) sont également exclues.
- Les champs `SITG_NPA` et `SITG_EGID` sont retournés en `String` par l'API et automatiquement convertis en `Int64`.
