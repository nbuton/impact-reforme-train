import itertools
import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from shapely.geometry import LineString
from tqdm import tqdm

# ===================== CONFIGURATION =====================
CSV_COMMUNES = "data/communes-france-2025.csv"
CSV_PEAGE = "data/gestionnaires-rrn-2025.csv"

NB_VILLES = 200  # Change ici (10 = test rapide, 50, 100...)

# Prefer environment variable instead of hard-coding secrets
API_KEY = os.getenv("SNCF_API_KEY", "")
if not API_KEY:
    raise RuntimeError("Missing SNCF_API_KEY environment variable")

PRIX_KM = 0.13
BUFFER_METRES = 1000

# Safe runtime budget: script may be stopped and resumed later
MAX_RUNTIME_HOURS = 48

# Regular temporary save
FLUSH_EVERY_N_RESULTS = 10
FLUSH_EVERY_SECONDS = 60

OSRM_URL = "https://router.project-osrm.org/route/v1/driving"
HEADERS = {"User-Agent": "peage-estimator/1.0"}

OUTPUT_CSV = f"comparatif_train_voiture_peage_{NB_VILLES}_villes.csv"
CHECKPOINT_JSON = f"comparatif_train_voiture_peage_{NB_VILLES}_villes.checkpoint.json"


# ===================== UTILITAIRES =====================
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_csv(df: pd.DataFrame, path: str) -> None:
    tmp_path = f"{path}.tmp"
    df.to_csv(tmp_path, index=False, sep=";", encoding="utf-8-sig")
    os.replace(tmp_path, path)


def save_checkpoint(
    checkpoint_path: str,
    cities: List[Dict],
    completed_pairs: int,
    last_pair: Optional[Tuple[str, str]],
    output_csv: str,
    extra: Optional[Dict] = None,
) -> None:
    payload = {
        "nb_villes": NB_VILLES,
        "output_csv": output_csv,
        "cities": cities,
        "completed_pairs": completed_pairs,
        "last_pair": list(last_pair) if last_pair else None,
        "updated_at_utc": now_utc_iso(),
    }
    if extra:
        payload.update(extra)

    tmp_path = f"{checkpoint_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, checkpoint_path)


def load_checkpoint(checkpoint_path: str) -> Optional[Dict]:
    path = Path(checkpoint_path)
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_existing_results(output_csv: str) -> pd.DataFrame:
    path = Path(output_csv)
    cols = ["ville_depart", "ville_arrivee", "distance_train", "distance_voiture", "tarif_peage"]
    if not path.exists():
        return pd.DataFrame(columns=cols)

    try:
        df = pd.read_csv(path, sep=";")
        for col in cols:
            if col not in df.columns:
                df[col] = pd.NA
        return df[cols].copy()
    except Exception:
        return pd.DataFrame(columns=cols)


# ===================== GÉOMÉTRIE / PÉAGE =====================
def lambert93_vers_wgs84(x: float, y: float) -> tuple:
    n = 0.7256077650
    c = 11754255.426
    xs = 700000.0
    ys = 12655612.050
    e = 0.0818191908
    lam0 = math.radians(3.0)

    r = math.sqrt((x - xs) ** 2 + (y - ys) ** 2)
    gamma = math.atan((x - xs) / (ys - y))

    lam = lam0 + gamma / n
    lat_iso = -1 / n * math.log(abs(r / c))

    phi = 2 * math.atan(math.exp(lat_iso)) - math.pi / 2
    for _ in range(15):
        e_sin = e * math.sin(phi)
        phi = 2 * math.atan(
            math.exp(lat_iso) * ((1 + e_sin) / (1 - e_sin)) ** (e / 2)
        ) - math.pi / 2

    return math.degrees(lam), math.degrees(phi)


def charger_sections_concedees(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, sep=";", low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    return df[df["concessionPrD"].astype(str).str.strip().str.upper() == "C"].copy()


def extraire_coordonnees(df: pd.DataFrame) -> list:
    segments = []
    colonnes_requises = {"xD", "yD", "xF", "yF", "longueur"}
    if not colonnes_requises.issubset(df.columns):
        return segments

    def to_float(v):
        return float(str(v).replace(",", "."))

    for _, row in df.iterrows():
        try:
            xd = to_float(row["xD"])
            yd = to_float(row["yD"])
            xf = to_float(row["xF"])
            yf = to_float(row["yF"])
            if any(math.isnan(v) for v in [xd, yd, xf, yf]):
                continue

            lon_d, lat_d = lambert93_vers_wgs84(xd, yd)
            lon_f, lat_f = lambert93_vers_wgs84(xf, yf)
            longueur_km = to_float(row["longueur"]) / 1000

            segments.append({
                "longueur_km": longueur_km,
                "geometry": LineString([(lon_d, lat_d), (lon_f, lat_f)]),
            })
        except Exception:
            continue
    return segments


def buffer_en_degres(metres: float, lat_ref: float = 46.5) -> float:
    return metres / (111320 * math.cos(math.radians(lat_ref)))


def calculer_km_concedes(itineraire_coords: list, segments: list) -> float:
    if not segments:
        return 0.0
    itineraire = LineString(itineraire_coords)
    lat_ref = itineraire_coords[len(itineraire_coords) // 2][1]
    buf_deg = buffer_en_degres(BUFFER_METRES, lat_ref)

    km_concedes = 0.0
    for seg in segments:
        milieu = seg["geometry"].centroid
        if itineraire.distance(milieu) <= buf_deg:
            km_concedes += seg["longueur_km"]
    return km_concedes


# ===================== REQUÊTES AVEC RETRY =====================
def request_json_with_retry(
    session: requests.Session,
    url: str,
    *,
    auth=None,
    params=None,
    timeout: int = 30,
    max_backoff: int = 900,
    label: str = "request",
    no_result_statuses: Optional[set] = None,
) -> Optional[dict]:
    """
    Fetch JSON with backoff.

    - 429 / 5xx => retry with exponential backoff.
    - Statuses in no_result_statuses => return None immediately.
    """
    no_result_statuses = no_result_statuses or set()
    backoff = 5

    while True:
        try:
            response = session.get(
                url,
                auth=auth,
                params=params,
                timeout=timeout,
                headers=HEADERS,
            )

            if response.status_code in no_result_statuses:
                return None

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_s = int(retry_after) if retry_after and retry_after.isdigit() else backoff
                print(f"  ⚠️  Rate limit sur {label}. Pause de {wait_s} s...")
                time.sleep(wait_s)
                backoff = min(backoff * 2, max_backoff)
                continue

            if response.status_code in {500, 502, 503, 504}:
                print(
                    f"  ⚠️  Erreur temporaire HTTP {response.status_code} sur {label}. "
                    f"Nouvelle tentative dans {backoff} s..."
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue

            response.raise_for_status()
            data = response.json()
            backoff = 5
            return data

        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in no_result_statuses:
                return None
            if status in {400, 401, 403, 404, 409, 422}:
                # Client error: do not loop forever.
                raise
            print(f"  ⚠️  Erreur HTTP sur {label}: {e}. Nouvelle tentative dans {backoff} s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

        except (requests.RequestException, ValueError) as e:
            print(f"  ⚠️  Erreur sur {label}: {e}. Nouvelle tentative dans {backoff} s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


def obtenir_itineraire(session: requests.Session, depart: tuple, arrivee: tuple) -> tuple:
    """Distance voiture via OSRM"""
    lat1, lon1 = depart
    lat2, lon2 = arrivee
    url = f"{OSRM_URL}/{lon1},{lat1};{lon2},{lat2}"
    params = {"overview": "full", "geometries": "geojson", "steps": "false"}

    data = request_json_with_retry(
        session,
        url,
        params=params,
        timeout=300,#30
        label="OSRM",
        no_result_statuses={404, 422},
    )

    if not data or data.get("code") != "Ok" or not data.get("routes"):
        raise RuntimeError("OSRM: aucun itinéraire trouvé")

    route = data["routes"][0]
    distance_km = route["distance"] / 1000
    coords = route["geometry"]["coordinates"]
    return coords, distance_km


def get_sncf_distance_train(session: requests.Session, from_admin: str, to_admin: str) -> float:
    """Appelle l'API SNCF et retourne la distance train réelle (km) du meilleur trajet"""
    params = {
        "from": from_admin,
        "to": to_admin,
        "datetime": "20260420T080000",
        "min_nb_transfers": 0,
        "count": 1,
        "first_section_mode[]": "walking",
        "last_section_mode[]": "walking",
    }
    url = "https://api.sncf.com/v1/coverage/sncf/journeys"

    data = request_json_with_retry(
        session,
        url,
        auth=(API_KEY, ""),
        params=params,
        timeout=20,
        label=f"SNCF {from_admin} -> {to_admin}",
        no_result_statuses={404, 422},
    )

    if not data or "journeys" not in data or not data["journeys"]:
        return 0.0

    journey = data["journeys"][0]
    total_km = 0.0
    for section in journey.get("sections", []):
        if section.get("type") == "public_transport" and "geojson" in section:
            geo = section["geojson"]
            if "properties" in geo and geo["properties"]:
                length_m = geo["properties"][0].get("length", 0)
                total_km += length_m / 1000

    return round(total_km, 1)


# ===================== MAIN =====================
def build_city_selection(df_communes: pd.DataFrame) -> List[Dict]:
    # Ensure coordinates are numeric
    for col in ["latitude_centre", "longitude_centre", "latitude_mairie", "longitude_mairie"]:
        if col in df_communes.columns:
            df_communes[col] = pd.to_numeric(
                df_communes[col].astype(str).str.replace(",", ".").str.strip(),
                errors="coerce",
            )

    df_top = df_communes.sort_values(by="population", ascending=False).head(NB_VILLES).copy()
    cities = []
    for _, row in df_top.iterrows():
        nom = str(row["nom_standard"]).strip()
        insee = str(row["code_insee"]).strip()
        cities.append({
            "nom_standard": nom,
            "code_insee": insee,
            "admin": f"admin:fr:{insee}",
        })
    return cities


def row_by_code_insee(df: pd.DataFrame, code_insee: str) -> pd.Series:
    matches = df[df["code_insee"].astype(str).str.strip() == str(code_insee).strip()]
    if matches.empty:
        raise KeyError(f"Commune introuvable pour code_insee={code_insee}")
    return matches.iloc[0]


def get_city_coords(row: pd.Series) -> Tuple[float, float]:
    lat = row["latitude_centre"] if not pd.isna(row.get("latitude_centre")) else row.get("latitude_mairie")
    lon = row["longitude_centre"] if not pd.isna(row.get("longitude_centre")) else row.get("longitude_mairie")
    if pd.isna(lat) or pd.isna(lon):
        raise ValueError("Coordonnées ville manquantes")
    return float(lat), float(lon)


def main():
    print("=== Comparatif Train (SNCF API) / Voiture / Péage ===")
    print(f"Nombre de villes configuré : {NB_VILLES}")
    print(f"Fichier de sortie : {OUTPUT_CSV}\n")

    start_time = time.monotonic()
    session = requests.Session()

    # 1) Chargement des communes
    df_communes = pd.read_csv(CSV_COMMUNES, low_memory=False)
    if "code_insee" not in df_communes.columns:
        raise RuntimeError("La colonne 'code_insee' est absente du fichier des communes")

    # 2) Chargement / création du checkpoint
    checkpoint = load_checkpoint(CHECKPOINT_JSON)
    if checkpoint and int(checkpoint.get("nb_villes", -1)) == NB_VILLES and checkpoint.get("cities"):
        cities = checkpoint["cities"]
        print(f"Checkpoint détecté : reprise de {len(cities)} villes.")
    else:
        cities = build_city_selection(df_communes)
        save_checkpoint(
            CHECKPOINT_JSON,
            cities=cities,
            completed_pairs=0,
            last_pair=None,
            output_csv=OUTPUT_CSV,
            extra={"created_at_utc": now_utc_iso(), "status": "initialized"},
        )
        print("Nouveau checkpoint créé.")

    print("Villes sélectionnées :", [c["nom_standard"] for c in cities[:10]])

    # 3) Chargement péage (une seule fois)
    print(f"\nChargement péage {CSV_PEAGE}...")
    df_concede = charger_sections_concedees(CSV_PEAGE)
    segments = extraire_coordonnees(df_concede)
    print(f"   → {len(segments)} segments concédés prêts.\n")

    # 4) Chargement des résultats déjà présents (reprise automatique)
    df_results = load_existing_results(OUTPUT_CSV)
    done_pairs = set(zip(df_results["ville_depart"].astype(str), df_results["ville_arrivee"].astype(str)))

    # 5) Préparation des paires
    city_names = [c["nom_standard"] for c in cities]
    combos = list(itertools.combinations(city_names, 2))
    total_pairs = len(combos)
    print(f"Calcul de {total_pairs} trajets (SNCF API + OSRM + péage local)...\n")

    # Map pour récupérer les lignes rapidement
    city_map = {c["nom_standard"]: c for c in cities}

    completed_since_flush = 0
    last_flush = time.monotonic()
    processed_pairs = len(done_pairs)

    try:
        for v1, v2 in tqdm(combos, desc="Trajets"):
            # Runtime guard: stop cleanly after the configured budget
            elapsed_hours = (time.monotonic() - start_time) / 3600
            if elapsed_hours >= MAX_RUNTIME_HOURS:
                print(f"\n⏹️  Limite de temps atteinte ({MAX_RUNTIME_HOURS} h). Sauvegarde et arrêt propre.")
                break

            if (v1, v2) in done_pairs:
                continue

            try:
                # ===================== TRAIN (API SNCF) =====================
                from_admin = city_map[v1]["admin"]
                to_admin = city_map[v2]["admin"]
                distance_train = get_sncf_distance_train(session, from_admin, to_admin)

                # ===================== VOITURE (OSRM) =====================
                row1 = row_by_code_insee(df_communes, city_map[v1]["code_insee"])
                row2 = row_by_code_insee(df_communes, city_map[v2]["code_insee"])

                lat1, lon1 = get_city_coords(row1)
                lat2, lon2 = get_city_coords(row2)

                coord_dep = (lat1, lon1)
                coord_arr = (lat2, lon2)
                itin_coords, distance_voiture = obtenir_itineraire(session, coord_dep, coord_arr)

                # ===================== PÉAGE (local) =====================
                km_concedes = calculer_km_concedes(itin_coords, segments)
                tarif_peage = round(km_concedes * PRIX_KM, 2)

                new_row = {
                    "ville_depart": v1,
                    "ville_arrivee": v2,
                    "distance_train": distance_train,
                    "distance_voiture": round(distance_voiture, 1),
                    "tarif_peage": tarif_peage,
                }

                # Avoid concat FutureWarning by appending row directly
                df_results.loc[len(df_results)] = new_row
                done_pairs.add((v1, v2))
                processed_pairs += 1
                completed_since_flush += 1

                # Flush régulier vers disque pour reprise à tout moment
                if (
                    completed_since_flush >= FLUSH_EVERY_N_RESULTS
                    or (time.monotonic() - last_flush) >= FLUSH_EVERY_SECONDS
                ):
                    atomic_write_csv(df_results, OUTPUT_CSV)
                    save_checkpoint(
                        CHECKPOINT_JSON,
                        cities=cities,
                        completed_pairs=processed_pairs,
                        last_pair=(v1, v2),
                        output_csv=OUTPUT_CSV,
                        extra={
                            "status": "running",
                            "progress_pairs": processed_pairs,
                            "total_pairs": total_pairs,
                            "last_flush_utc": now_utc_iso(),
                        },
                    )
                    completed_since_flush = 0
                    last_flush = time.monotonic()

                # Petite pause de courtoisie pour éviter de trop marteler les APIs
                time.sleep(1.0)

            except requests.HTTPError as e:
                # No-route / no-journey cases should just be skipped
                print(f"  Erreur HTTP sur {v1}-{v2} : {e}")
                save_checkpoint(
                    CHECKPOINT_JSON,
                    cities=cities,
                    completed_pairs=processed_pairs,
                    last_pair=(v1, v2),
                    output_csv=OUTPUT_CSV,
                    extra={
                        "status": "error_continue",
                        "last_error": str(e),
                        "progress_pairs": processed_pairs,
                        "total_pairs": total_pairs,
                        "updated_at_utc": now_utc_iso(),
                    },
                )
                continue

            except Exception as e:
                # On logge l'erreur et on continue. Le checkpoint évite de perdre le reste.
                print(f"  Erreur sur {v1}-{v2} : {e}")
                save_checkpoint(
                    CHECKPOINT_JSON,
                    cities=cities,
                    completed_pairs=processed_pairs,
                    last_pair=(v1, v2),
                    output_csv=OUTPUT_CSV,
                    extra={
                        "status": "error_continue",
                        "last_error": str(e),
                        "progress_pairs": processed_pairs,
                        "total_pairs": total_pairs,
                        "updated_at_utc": now_utc_iso(),
                    },
                )
                continue

        # Sauvegarde finale
        atomic_write_csv(df_results, OUTPUT_CSV)
        last_pair = None
        if done_pairs:
            last_pair = None

        save_checkpoint(
            CHECKPOINT_JSON,
            cities=cities,
            completed_pairs=len(done_pairs),
            last_pair=last_pair,
            output_csv=OUTPUT_CSV,
            extra={
                "status": "completed",
                "completed_at_utc": now_utc_iso(),
                "total_pairs": total_pairs,
                "result_rows": len(df_results),
            },
        )

        print(f"\n✅ Terminé ! {len(df_results)} trajets enregistrés dans '{OUTPUT_CSV}'")
        print("   Reprise automatique possible via le checkpoint et le CSV partiel.")
        print("   Distance train = distance réelle calculée par l'API SNCF officielle")

    except KeyboardInterrupt:
        # Sauvegarde propre pour reprise immédiate plus tard
        atomic_write_csv(df_results, OUTPUT_CSV)
        save_checkpoint(
            CHECKPOINT_JSON,
            cities=cities,
            completed_pairs=len(done_pairs),
            last_pair=None,
            output_csv=OUTPUT_CSV,
            extra={
                "status": "interrupted",
                "interrupted_at_utc": now_utc_iso(),
                "total_pairs": total_pairs,
                "result_rows": len(df_results),
            },
        )
        print("\n⏸️  Interruption capturée. État sauvegardé pour reprise ultérieure.")


if __name__ == "__main__":
    main()
