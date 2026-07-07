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
      "addressId": "930225017479",
      "streetName": "Avenue Jacob-Daniel-MAILLARD",
      "houseNumber": "7",
      "postalCode": "1217",
      "locality": "Meyrin",
      "municipality": "Meyrin",
      "type": "Existante",
      "EGID": "1021280",
      "EGRID": "CH276384676537",
      "longitude": 6.068229,
      "latitude": 46.22923,
      "coordinates": {
        "x": 2494278.54,
        "y": 1120679.36,
        "crs": "EPSG:2056"
      },
      "administrativeDivision": "Canton de Genève",
      "country": "Suisse",
      "indexed_at": "2026-07-07T04:00:15.126683+00:00",
      "score": 100.0,
      "dataSource": {
        "provider": "Système d'Information du Territoire à Genève (SITG)",
        "metadataUrl": "https://sitg.ge.ch/donnees/cad-adresse",
        "dataUrl": "https://ge.ch/sitg/geodata/SITG/OPENDATA/CAD_ADRESSE-SHP.zip!CAD_ADRESSE.shp",
        "retrievedAt": "2026-07-07T04:00:05.457239+00:00"
      },
      "formatted": null
    }
  ],
  "query": "7 avenue jacob-daniel-maillard meyrin",
  "processingTimeMs": 1,
  "limit": 1,
  "offset": 0,
  "nbHits": 1000
}
```

> Le client interroge `/api/v2/search`. L'ancien endpoint (`/api/search`, schéma v1 avec `IDPADR`/`ADRESSE`/`NO_POSTAL`) ne retourne plus de `score` fiable et a été abandonné.
>
> Limite connue de l'API : les adresses abrégées (ex. `"Av. J-D Maillard, 7, Meyrin"`) peuvent matcher un mauvais résultat avec un score faible — préférer la forme développée (`"Avenue Jacob-Daniel-Maillard 7, Meyrin"`) si le score est anormalement bas malgré une adresse correcte.

### Géocoder une liste d'adresses

```python
import polars as pl
from sitg_geocode import sitg_geocode_async

df = pl.read_csv("test_adresses.csv")

result_geocode = await sitg_geocode_async(
    df,
    col_adresse="Rue et N°",
    max_concurrent=10,
    min_score_threshold=95,
)

# Joindre les résultats au DataFrame original
df_resultat = df.join(result_geocode, on="Rue et N°", how="left")
df_resultat.write_csv("resultat.csv")

print(df_resultat.head(5))
```

Résultat (`min_score_threshold=95`) : 

| Rue et N° | SITG_ADRESSE | SITG_NPA | SITG_COMMUNE | SITG_TYPE | SITG_CANTON | SITG_PAYS | SITG_EGID | SITG_SCORE | SITG_PROVIDER |
|-----------|--------------|----------|--------------|-----------|-------------|-----------|-----------|-----------|---------------|
| "12 rue Jean-Charles Amat" | null | null | null | null | null | null | null | null | null |
| "37, sous Garan" | null | null | null | null | null | null | null | null | null |
| "Ancienne route, 78" | null | null | null | null | null | null | null | null | null |
| "Av de Thonex 30" | "Avenue de Thônex 30" | 1225 | "Chêne-Bourg" | "Existante" | "Canton de Genève" | "Suisse" | 295511715 | 100.0 | "SITG" |
| "Av. Blanc, 5" | "Avenue du Mont Blanc 5" | 74950 | "Scionzier" | null | "Département de la Haute-Savoie" | "France" | null | 100.0 | "Base Adresse Nationale (BAN)" |

> Avec `min_score_threshold=0` (aucun filtre), les 5 adresses trouvent bien une correspondance (scores 70–100), mais l'API v2 attribue des scores globalement plus bas que l'ancienne v1 pour les correspondances imparfaites (ex. "12 rue Jean-Charles Amat" → 75.47 au lieu de 95.83 précédemment). Si vous migrez depuis une version antérieure, réévaluez votre `min_score_threshold` plutôt que de réutiliser l'ancien seuil tel quel.

## Paramètres de `sitg_geocode_async`

| Paramètre              | Type              | Défaut      | Description                                                                 |
|------------------------|-------------------|-------------|-----------------------------------------------------------------------------|
| `df`                   | `pl.DataFrame`    | —           | DataFrame en entrée (Polars)                                                |
| `col_adresse`          | `str`             | —           | Nom de la colonne contenant les adresses à géocoder                         |
| `max_concurrent`       | `int`             | `10`        | Nombre maximum de requêtes HTTP simultanées                                 |
| `min_score_threshold`  | `float`           | `0.0`       | Score minimum pour conserver un résultat (0–100). `0.0` = conserver tout    |

## Colonnes retournées

Le DataFrame résultat de l'exemple `df_resultat` contient la colonne d'adresse originale (clé de jointure) et les champs suivants :

| Colonne                | Type       | Description                                          |
|------------------------|------------|------------------------------------------------------|
| `SITG_ADRESSE_ID`      | `String`   | Identifiant interne de l'adresse (API)               |
| `SITG_ADRESSE`         | `String`   | Adresse normalisée (rue + numéro)                    |
| `SITG_NPA`             | `Int64`    | Code postal                                          |
| `SITG_NOM_NPA`         | `String`   | Nom de la localité                                   |
| `SITG_COMMUNE`         | `String`   | Commune                                              |
| `SITG_TYPE`            | `String`   | Statut du bâtiment (ex. `Existante`, `Projetée`)     |
| `SITG_CANTON`          | `String`   | Division administrative (canton / département)       |
| `SITG_PAYS`            | `String`   | Pays                                                 |
| `SITG_EGID`            | `Int64`    | EGID - Identifiant fédéral du bâtiment (RegBL)       |
| `SITG_EGRID`           | `String`   | EGRID - Identifiant fédéral de l'immeuble (RF)       |
| `SITG_SCORE`           | `Float64`  | Score de confiance du géocodage (0–100)              |
| `SITG_PROVIDER`        | `String`   | Source des données (ex. SITG, RegBL, BAN)            |
| `SITG_LON`             | `Float64`  | Longitude WGS84                                      |
| `SITG_LAT`             | `Float64`  | Latitude WGS84                                       |
| `SITG_EST_EPSG_2056`   | `Float64`  | Coordonnée Est LV95 / EPSG:2056                      |
| `SITG_NORD_EPSG_2056`  | `Float64`  | Coordonnée Nord LV95 / EPSG:2056                     |

> `SITG_EGID`/`SITG_EGRID` peuvent être `null` même pour un résultat de haute confiance : certaines sources (ex. `Base Adresse Nationale (BAN)` pour les adresses françaises) ne fournissent pas ces identifiants fédéraux suisses. Utilisez `SITG_PROVIDER` pour comprendre pourquoi.

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
