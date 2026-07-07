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

import aiohttp
import dataframely as dy
import polars as pl
from loguru import logger
from tqdm.auto import tqdm

API_URL = "https://geocodage.sitg-lab.ch/api/v2/search"


# Schéma attendu après transformation — source de vérité.
# Toutes les colonnes sont nullables : le géocodage peut échouer pour une adresse.
class SitgGeocodeSchema(dy.Schema):
    SITG_ADRESSE_ID = dy.String(nullable=True)
    SITG_ADRESSE = dy.String(nullable=True)
    SITG_NPA = dy.Int64(nullable=True)
    SITG_NOM_NPA = dy.String(nullable=True)
    SITG_COMMUNE = dy.String(nullable=True)
    SITG_TYPE = dy.String(nullable=True)
    SITG_CANTON = dy.String(nullable=True)
    SITG_PAYS = dy.String(nullable=True)
    SITG_EGID = dy.Int64(nullable=True)
    SITG_EGRID = dy.String(nullable=True)
    SITG_SCORE = dy.Float64(nullable=True, min=0.0, max=100.0)
    SITG_PROVIDER = dy.String(nullable=True)
    SITG_LON = dy.Float64(nullable=True)
    SITG_LAT = dy.Float64(nullable=True)
    SITG_EST_EPSG_2056 = dy.Float64(nullable=True)
    SITG_NORD_EPSG_2056 = dy.Float64(nullable=True)


_RESULT_FIELDS = SitgGeocodeSchema.column_names()
_EMPTY_RESULT = {f: None for f in _RESULT_FIELDS}


def validate_schema(df: pl.DataFrame) -> list[str]:
    """
    Vérifie que le DataFrame respecte SitgGeocodeSchema (colonnes, types, et
    règles de contenu comme le score dans [0, 100]) via dataframely.
    Retourne une liste d'erreurs (vide = OK).
    """
    try:
        _, failure = SitgGeocodeSchema.filter(df, cast=True)
    except Exception as e:
        return [str(e)]
    if failure.counts():
        return [f"{rule}: {count} ligne(s) invalide(s)" for rule, count in failure.counts().items()]
    return []


def _select_hit(hits: list[dict], npa_range: tuple[int, int] | None) -> dict | None:
    """Sélectionne le premier hit dont le NPA tombe dans npa_range (ou le meilleur hit si None)."""
    if not hits:
        return None
    if npa_range is None:
        return hits[0]
    npa_min, npa_max = npa_range
    for hit in hits:
        try:
            npa = int(hit.get("postalCode"))
        except (TypeError, ValueError):
            continue
        if npa_min <= npa <= npa_max:
            return hit
    return None


def _hit_to_result(hit: dict) -> dict:
    coordinates = hit.get("coordinates") or {}
    data_source = hit.get("dataSource") or {}
    street_name = hit.get("streetName")
    house_number = hit.get("houseNumber")
    sitg_adresse = f"{street_name} {house_number}".strip() if street_name or house_number else None
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


async def _fetch_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    adresse: str,
    npa_range: tuple[int, int] | None = None,
) -> dict:
    """Geocode une adresse et retourne les champs du
    meilleur résultat dans npa_range (si fourni)."""
    # Avec une restriction NPA, on demande plusieurs candidats : le premier résultat de l'API
    # n'est pas forcément dans la plage voulue (ex. une adresse française mieux scorée).
    limit = 10 if npa_range is not None else 1
    params = {"q": adresse, "limit": str(limit), "offset": "0", "suggest": "false"}
    try:
        async with semaphore, session.get(API_URL, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
        hits = data.get("hits") or []
        hit = _select_hit(hits, npa_range)
        if hit is not None:
            return _hit_to_result(hit)
        if hits and npa_range is not None:
            logger.warning(
                "Aucun résultat dans la plage NPA {}-{} pour '{}' ({} résultat(s) hors plage)",
                npa_range[0],
                npa_range[1],
                adresse,
                len(hits),
            )
        else:
            logger.warning("Adresse introuvable (aucun résultat SITG) : '{}'", adresse)
    except Exception as e:
        logger.warning("Échec de la requête de géocodage pour '{}' : {}", adresse, e)
    return _EMPTY_RESULT.copy()


async def sitg_geocode_async(
    df: pl.DataFrame,
    col_adresse: str,
    max_concurrent: int = 10,
    min_score_threshold: float = 0.0,
    npa_range: tuple[int, int] | None = None,
) -> pl.DataFrame:
    """
    Géocode une colonne d'adresses via l'API SITG Lab.

    Paramètres
    ----------
    df            : DataFrame Polars
    col_adresse   : nom de la colonne contenant les adresses
    max_concurrent: nombre max de requêtes HTTP simultanées
    min_score_threshold: seuil minimum de score pour considérer un résultat comme valide
    npa_range     : (min, max) inclusif ; parmi les résultats retournés par l'API pour une
                    adresse, ne retient que le premier dont le NPA tombe dans cette plage
                    (ex. (1200, 1299) pour ne garder que le canton de Genève). Si aucun des
                    résultats n'est dans la plage, l'adresse est considérée non géocodée.
                    `None` (défaut) = pas de restriction géographique.
    Retourne
    --------
    DataFrame avec la colonne adresse + les champs SITG définis dans SitgGeocodeSchema.
    Les types sont validés (et castés) et les écarts éventuels sont loggés.
    """
    adresses = df[col_adresse].to_list()

    semaphore = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_one(session, semaphore, addr, npa_range) for addr in adresses]
        results = await tqdm.gather(*tasks, desc="Geocoding SITG")

    sitg_fields = pl.DataFrame(
        {f: [r[f] for r in results] for f in _RESULT_FIELDS},
        infer_schema_length=None,
    )
    # NPA et EGID sont retournés en String par l'API : dataframely les caste en Int64
    # et vérifie au passage que toutes les colonnes/types attendus sont bien présents.
    sitg_fields = SitgGeocodeSchema.cast(sitg_fields)

    # Les règles de contenu (ex. score dans [0, 100]) ne bloquent jamais le résultat :
    # elles sont uniquement loguées, pour ne pas faire disparaître de lignes en silence.
    _, failure = SitgGeocodeSchema.filter(sitg_fields)
    for rule, count in failure.counts().items():
        logger.warning("Règle de schéma violée '{}' : {} ligne(s)", rule, count)

    result = pl.DataFrame({col_adresse: adresses}).hstack(sitg_fields)

    # Appliquer un filtre de score minimum si spécifié
    if min_score_threshold > 0:
        result = result.filter(pl.col("SITG_SCORE") >= min_score_threshold)

    n_not_found = sum(1 for r in results if r == _EMPTY_RESULT)
    if n_not_found:
        logger.warning(
            "{}/{} adresse(s) non géocodée(s) (voir les avertissements ci-dessus)",
            n_not_found,
            len(results),
        )
    else:
        logger.info("{}/{} adresse(s) géocodée(s) avec succès", len(results), len(results))

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
