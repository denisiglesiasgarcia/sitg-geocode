"""
sitg_geocode.geocode
--------------------
Geocodage asynchrone d'adresses via l'API SITG Lab (Genève).

API : https://geocodage.sitg-lab.ch

Fonctions exposées
------------------
- :func:`sitg_geocode_async` : géocode une colonne d'adresses.
- :func:`inspect_sitg_response` : affiche la réponse brute de l'API.
- :func:`validate_schema` : vérifie le schéma de sortie attendu.

Résultats retournés
-------------------
Colonne                 Description
----------------------  --------------------------------------------------
col_adresse             Adresse originale (clé de jointure)
SITG_ADRESSE_ID         Identifiant interne de l'adresse (API)
SITG_ADRESSE            Adresse normalisée SITG
SITG_NPA                Code postal
SITG_NOM_NPA            Nom de la localité
SITG_COMMUNE            Commune
SITG_TYPE               Statut du bâtiment (ex. Existante, Projetée)
SITG_CANTON             Division administrative (ex. Canton de Genève)
SITG_PAYS               Pays
SITG_EGID               Identifiant fédéral du bâtiment (RegBL)
SITG_EGRID              Identifiant fédéral de l'immeuble (RF)
SITG_SCORE              Score de confiance du géocodage (0–100)
SITG_PROVIDER           Source des données (ex. SITG, RegBL)
SITG_LON / SITG_LAT     Coordonnées géographiques WGS84
SITG_EST_EPSG_2056      Coordonnée Est  LV95 / EPSG:2056
SITG_NORD_EPSG_2056     Coordonnée Nord LV95 / EPSG:2056
"""

import asyncio
import json
import logging

import aiohttp
import polars as pl
from tqdm.auto import tqdm

logger = logging.getLogger(__name__)

API_URL = "https://geocodage.sitg-lab.ch/api/v2/search"

_RESULT_FIELDS = [
    "SITG_ADRESSE_ID",
    "SITG_ADRESSE",
    "SITG_NPA",
    "SITG_NOM_NPA",
    "SITG_COMMUNE",
    "SITG_TYPE",
    "SITG_CANTON",
    "SITG_PAYS",
    "SITG_EGID",
    "SITG_EGRID",
    "SITG_SCORE",
    "SITG_PROVIDER",
    "SITG_LON",
    "SITG_LAT",
    "SITG_EST_EPSG_2056",
    "SITG_NORD_EPSG_2056",
]

_EMPTY_RESULT = {f: None for f in _RESULT_FIELDS}

# Schéma attendu après transformation — source de vérité
EXPECTED_SCHEMA: dict[str, type[pl.DataType]] = {
    "SITG_ADRESSE_ID": pl.String,
    "SITG_ADRESSE": pl.String,
    "SITG_NPA": pl.Int64,
    "SITG_NOM_NPA": pl.String,
    "SITG_COMMUNE": pl.String,
    "SITG_TYPE": pl.String,
    "SITG_CANTON": pl.String,
    "SITG_PAYS": pl.String,
    "SITG_EGID": pl.Int64,
    "SITG_EGRID": pl.String,
    "SITG_SCORE": pl.Float64,
    "SITG_PROVIDER": pl.String,
    "SITG_LON": pl.Float64,
    "SITG_LAT": pl.Float64,
    "SITG_EST_EPSG_2056": pl.Float64,
    "SITG_NORD_EPSG_2056": pl.Float64,
}

# Toutes les colonnes résultat sont nullable (le géocodage peut échouer)
NULLABLE_COLUMNS: frozenset[str] = frozenset(EXPECTED_SCHEMA.keys())


def validate_schema(df: pl.DataFrame) -> list[str]:
    """
    Vérifie que le DataFrame respecte EXPECTED_SCHEMA.
    Retourne une liste d'erreurs (vide = OK).
    """
    errors: list[str] = []
    for col, expected_dtype in EXPECTED_SCHEMA.items():
        if col not in df.columns:
            errors.append(f"{col}: colonne absente")
            continue
        actual = df[col].dtype
        if actual == pl.Null and col in NULLABLE_COLUMNS:
            continue
        if actual != expected_dtype:
            errors.append(f"{col}: attendu {expected_dtype}, obtenu {actual}")
    return errors


async def _fetch_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    adresse: str,
) -> dict:
    """Geocode une adresse et retourne les champs du meilleur résultat."""
    params = {"q": adresse, "limit": "1", "offset": "0", "suggest": "false"}
    try:
        async with semaphore, session.get(API_URL, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
        if data.get("hits"):
            hit = data["hits"][0]
            coordinates = hit.get("coordinates") or {}
            data_source = hit.get("dataSource") or {}
            street_name = hit.get("streetName")
            house_number = hit.get("houseNumber")
            sitg_adresse = (
                f"{street_name} {house_number}".strip() if street_name or house_number else None
            )
            return {
                "SITG_ADRESSE_ID": hit.get("addressId"),
                "SITG_ADRESSE": sitg_adresse,
                "SITG_NPA": hit.get("postalCode"),
                "SITG_NOM_NPA": hit.get("locality"),
                "SITG_COMMUNE": hit.get("municipality"),
                "SITG_TYPE": hit.get("type"),
                "SITG_CANTON": hit.get("administrativeDivision"),
                "SITG_PAYS": hit.get("country"),
                "SITG_EGID": hit.get("EGID"),
                "SITG_EGRID": hit.get("EGRID"),
                "SITG_SCORE": hit.get("score"),
                "SITG_PROVIDER": data_source.get("provider"),
                "SITG_LON": hit.get("longitude"),
                "SITG_LAT": hit.get("latitude"),
                "SITG_EST_EPSG_2056": coordinates.get("x"),
                "SITG_NORD_EPSG_2056": coordinates.get("y"),
            }
    except Exception as e:
        logger.warning("Geocoding failed for '%s': %s", adresse, e)
    return _EMPTY_RESULT.copy()


async def sitg_geocode_async(
    df: pl.DataFrame,
    col_adresse: str,
    max_concurrent: int = 10,
    min_score_threshold: float = 0.0,
) -> pl.DataFrame:
    """
    Géocode une colonne d'adresses via l'API SITG Lab.

    Paramètres
    ----------
    df            : DataFrame Polars
    col_adresse   : nom de la colonne contenant les adresses
    max_concurrent: nombre max de requêtes HTTP simultanées
    min_score_threshold: seuil minimum de score pour considérer un résultat comme valide
    Retourne
    --------
    DataFrame avec la colonne adresse + les champs SITG définis dans EXPECTED_SCHEMA.
    Les types sont validés et loggés si un écart est détecté.
    """
    adresses = df[col_adresse].to_list()

    semaphore = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_one(session, semaphore, addr) for addr in adresses]
        results = await tqdm.gather(*tasks, desc="Geocoding SITG")

    result = pl.DataFrame(
        {col_adresse: adresses, **{f: [r[f] for r in results] for f in _RESULT_FIELDS}},
        infer_schema_length=None,
    ).with_columns(
        # NPA et EGID sont retournés en String par l'API
        pl.col("SITG_NPA", "SITG_EGID").cast(pl.Int64),
    )

    schema_errors = validate_schema(result)
    for err in schema_errors:
        logger.warning("Geocode schema mismatch: %s", err)

    # Appliquer un filtre de score minimum si spécifié
    if min_score_threshold > 0:
        result = result.filter(pl.col("SITG_SCORE") >= min_score_threshold)

    return result


async def inspect_sitg_response(adresse: str, **params_override: str) -> None:
    """Affiche la réponse brute de l'API pour une adresse donnée.

    params_override permet de remplacer/ajouter des paramètres de requête,
    par ex. inspect_sitg_response(adresse, suggest="true") pour comparer
    le comportement avec suggest=false.
    """
    params: dict[str, str] = {"q": adresse, "limit": "1", "offset": "0", "suggest": "false"}
    params.update(params_override)
    async with (
        aiohttp.ClientSession() as session,
        session.get(API_URL, params=params) as resp,
    ):
        data = await resp.json()
    print(json.dumps(obj=data, indent=2, ensure_ascii=False))
