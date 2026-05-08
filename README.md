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
uv add git+https://github.com/denisiglesiasgarcia/sitg-geocodage

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

<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (5, 12)</small><table border="1" class="dataframe"><thead><tr><th>Rue et N°</th><th>SITG_ADRESSE</th><th>SITG_NPA</th><th>SITG_NOM_NPA</th><th>SITG_COMMUNE</th><th>SITG_EGID</th><th>SITG_EGRID</th><th>SITG_SCORE</th><th>SITG_LON</th><th>SITG_LAT</th><th>SITG_EST_EPSG_2056</th><th>SITG_NORD_EPSG_2056</th></tr><tr><td>str</td><td>str</td><td>i64</td><td>str</td><td>str</td><td>i64</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>&quot;12 rue Jean-Charles Amat&quot;</td><td>&quot;Rue Jean-Charles-AMAT 12&quot;</td><td>1202</td><td>&quot;Genève&quot;</td><td>&quot;Genève-Petit-Saconnex&quot;</td><td>2037721</td><td>&quot;CH168165638919&quot;</td><td>95.83</td><td>6.148414</td><td>46.21442</td><td>2.5004e6</td><td>1.1189e6</td></tr><tr><td>&quot;37, sous Garan&quot;</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td></tr><tr><td>&quot;Ancienne route, 78&quot;</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td></tr><tr><td>&quot;Av de Thonex 30&quot;</td><td>&quot;Avenue de Thônex 30&quot;</td><td>1225</td><td>&quot;Chêne-Bourg&quot;</td><td>&quot;Chêne-Bourg&quot;</td><td>295511715</td><td>&quot;CH576391476529&quot;</td><td>100.0</td><td>6.198379</td><td>46.19087</td><td>2.5043e6</td><td>1.1162e6</td></tr><tr><td>&quot;Av. Blanc, 5&quot;</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td></tr></tbody></table></div>

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