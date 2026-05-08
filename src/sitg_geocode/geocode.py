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
SITG_ADRESSE            Adresse normalisée SITG
SITG_NPA                Code postal
SITG_COMMUNE            Commune
SITG_EGID               Identifiant fédéral du bâtiment (RegBL)
SITG_EGRID              Identifiant fédéral de l'immeuble (RF)
SITG_SCORE              Score de confiance du géocodage (0–100)
SITG_LON / SITG_LAT     Coordonnées géographiques WGS84
SITG_EST_EPSG_2056      Coordonnée Est  LV95 / EPSG:2056
SITG_NORD_EPSG_2056     Coordonnée Nord LV95 / EPSG:2056
"""

import asyncio
import json
import logging
from typing import Literal

import aiohttp
import pandas as pd
import polars as pl
from tqdm.auto import tqdm

logger = logging.getLogger(__name__)

API_URL = "https://geocodage.sitg-lab.ch/api/search"

_RESULT_FIELDS = [
    "SITG_ADRESSE",
    "SITG_NPA",
    "SITG_NOM_NPA",
    "SITG_COMMUNE",
    "SITG_EGID",
    "SITG_EGRID",
    "SITG_SCORE",
    "SITG_LON",
    "SITG_LAT",
    "SITG_EST_EPSG_2056",
    "SITG_NORD_EPSG_2056",
]

_EMPTY_RESULT = {f: None for f in _RESULT_FIELDS}

# Schéma attendu après transformation — source de vérité
EXPECTED_SCHEMA: dict[str, pl.DataType] = {
    "SITG_ADRESSE": pl.String,
    "SITG_NPA": pl.Int64,
    "SITG_NOM_NPA": pl.String,
    "SITG_COMMUNE": pl.String,
    "SITG_EGID": pl.Int64,
    "SITG_EGRID": pl.String,
    "SITG_SCORE": pl.Float64,
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
            return {
                "SITG_ADRESSE": hit.get("ADRESSE"),
                "SITG_NPA": hit.get("NO_POSTAL"),
                "SITG_NOM_NPA": hit.get("NOM_NPA"),
                "SITG_COMMUNE": hit.get("COMMUNE"),
                "SITG_EGID": hit.get("EGID"),
                "SITG_EGRID": hit.get("EGRID"),
                "SITG_SCORE": hit.get("score"),
                "SITG_LON": hit.get("longitude"),
                "SITG_LAT": hit.get("latitude"),
                "SITG_EST_EPSG_2056": hit.get("easting"),
                "SITG_NORD_EPSG_2056": hit.get("northing"),
            }
    except Exception as e:
        logger.warning("Geocoding failed for '%s': %s", adresse, e)
    return _EMPTY_RESULT.copy()


async def sitg_geocode_async(
    df: pl.DataFrame | pd.DataFrame,
    col_adresse: str,
    output_format: Literal["polars", "pandas"] = "polars",
    max_concurrent: int = 10,
    min_score_threshold: float = 0.0,
) -> pl.DataFrame | pd.DataFrame:
    """
    Géocode une colonne d'adresses via l'API SITG Lab.

    Paramètres
    ----------
    df            : DataFrame Polars ou Pandas en entrée
    col_adresse   : nom de la colonne contenant les adresses
    output_format : "polars" (défaut) ou "pandas"
    max_concurrent: nombre max de requêtes HTTP simultanées
    min_score_threshold: seuil minimum de score pour considérer un résultat comme valide
    Retourne
    --------
    DataFrame avec la colonne adresse + les champs SITG définis dans EXPECTED_SCHEMA.
    Les types sont validés et loggés si un écart est détecté.
    """
    adresses = (
        df[col_adresse].to_list()
        if isinstance(df, pl.DataFrame)
        else df[col_adresse].tolist()
    )

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

    return result.to_pandas() if output_format == "pandas" else result


async def inspect_sitg_response(adresse: str) -> None:
    """Affiche la réponse brute de l'API pour une adresse donnée."""
    params = {"q": adresse, "limit": "1", "offset": "0", "suggest": "false"}
    async with (
        aiohttp.ClientSession() as session,
        session.get(API_URL, params=params) as resp,
    ):
        data = await resp.json()
    print(json.dumps(data, indent=2, ensure_ascii=False))
