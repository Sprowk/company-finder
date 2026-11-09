#!/usr/bin/env python3
import os
import re
import csv
import gzip
import unicodedata
import requests
from datetime import datetime
from pathlib import Path

RPO_DUMP_URL = os.getenv("RPO_DUMP_URL")

ROOT = Path(__file__).resolve().parent
SNAP_DIR = ROOT / "snapshots"
SNAP_DIR.mkdir(exist_ok=True)

DATA_DIR = ROOT / "data"
OBCE_CSV_PATH = DATA_DIR / "obce.csv"  # číselník obcí: mesto -> kraj

TMP_DUMP_PATH = Path("/tmp/rpo.sql.gz")

ROWS_PER_PART = 100_000   # doladíš podľa veľkosti výstupu


# --------- pomocné ---------

def download_dump(url: str, dest: Path):
    print(f"📥 Sťahujem dump z {url} ...")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    print(f"✅ Stiahnuté do {dest}")


def open_part_writer(base_name: str, part_index: int):
    part_str = f"{part_index:02d}"
    out_path = SNAP_DIR / f"{base_name}_part{part_str}.csv.gz"
    f = gzip.open(out_path, "wt", encoding="utf-8", newline="")
    writer = csv.writer(f)
    return out_path, f, writer


def _to_dmy(val: str) -> str:
    if not val:
        return ""
    date_part = val.split(" ")[0].split("T")[0]
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", date_part)
    if not m:
        m2 = re.match(r"^(\d{4})-(\d{2})-(\d{2}).*", date_part)
        if not m2:
            return date_part
        yyyy, mm, dd = m2.group(1), m2.group(2), m2.group(3)
        return f"{dd}.{mm}.{yyyy}"
    yyyy, mm, dd = m.group(1), m.group(2), m.group(3)
    return f"{dd}.{mm}.{yyyy}"


# --------- normalizácia názvov miest ---------

def normalize_city_key(s: str) -> str:
    """
    Normalizuje názov mesta pre porovnávanie:
    - odstráni diakritiku
    - všetko na lower
    - pomlčky rôznych typov → medzera
    - znormalizuje viacnásobné medzery
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # bez diakritiky
    s = s.lower()
    s = re.sub(r"[-–—]", " ", s)   # pomlčky → medzera
    s = re.sub(r"\s+", " ", s)     # viac medzier → jedna
    return s.strip()


# --------- mapovanie mesto → kraj ---------

def load_city_region_map():
    """
    Načíta mapu {NORMALIZED_KEY: KRAJ} z data/obce.csv.

    Očakávaný formát BEZ HLAVIČKY:
      názov;okres;kraj;lat;lon

    Príklad riadku:
      Abrahám;Galanta;Trnavský kraj;48.248383;17.618816

    Pre názvy s pomlčkou ("Bratislava - Ružinov") pridáme aliasy:
      - "bratislava ruzinov"
      - "bratislava"
      - "ruzinov"
    """
    city_to_region = {}
    if not OBCE_CSV_PATH.exists():
        print(f"⚠️ Varovanie: {OBCE_CSV_PATH} neexistuje, mapovanie mesto → kraj bude prázdne.")
        return city_to_region

    with OBCE_CSV_PATH.open("r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")
        for line_no, row in enumerate(reader, start=1):
            if not row or len(row) < 3:
                continue

            # keby sa časom objavila hlavička
            if line_no == 1 and (
                "kraj" in row[2].lower()
                or "názov" in row[0].lower()
                or "nazov" in row[0].lower()
            ):
                continue

            name = (row[0] or "").strip()
            region = (row[2] or "").strip()
            if not name or not region:
                continue

            main_key = normalize_city_key(name)
            if main_key and main_key not in city_to_region:
                city_to_region[main_key] = region

            parts = re.split(r"\s*[-–—]\s*", name)
            if len(parts) >= 2:
                for p in parts:
                    p = p.strip()
                    if not p:
                        continue
                    k = normalize_city_key(p)
                    if k and k not in city_to_region:
                        city_to_region[k] = region

    print(f"🗺️ Načítaných {len(city_to_region)} normalizovaných kľúčov obcí → kraj z {OBCE_CSV_PATH}")
    return city_to_region


def guess_region(city: str, city_region_map):
    """
    Vráti kraj podľa názvu mesta z mapy obcí.
    Používa normalizovaný kľúč (bez diakritiky, toleruje pomlčky).
    Ak sa mesto nenájde, vráti prázdny string.
    """
    if not city:
        return ""
    key = normalize_city_key(city)
    region = city_region_map.get(key)
    if region:
        return region
    # skúsime rozdeliť vstup (napr. "Bratislava - mestská časť Petržalka") podľa pomlčky
    # a pre každú časť skúsiť nájsť kraj v číselníku obcí
    for part in re.split(r"\s*[-–—]\s*", city):
        part_key = normalize_city_key(part)
        if not part_key:
            continue
        region = city_region_map.get(part_key)
        if region:
            return region
    return ""


# --------- parsovanie názvov ---------

def _better_name(old, new):
    if old is None:
        return True
    if (old.get("effective_to") is None) != (new.get("effective_to") is None):
        return new.get("effective_to") is None
    def ts(x): return x or ""
    if ts(new.get("effective_from")) != ts(old.get("effective_from")):
        return ts(new.get("effective_from")) > ts(old.get("effective_from"))
    if ts(new.get("updated_at")) != ts(old.get("updated_at")):
        return ts(new.get("updated_at")) > ts(old.get("updated_at"))
    return False


def parse_names_map(dump_path: Path):
    print("🔎 Parsujem názvy z rpo.organization_name_entries ...")
    in_copy = False
    col_lc = []
    idx_org = idx_name = idx_eff_from = idx_eff_to = idx_updated = None
    best = {}

    with gzip.open(dump_path, "rt", encoding="utf-8", newline="") as gz:
        for raw in gz:
            line = raw.rstrip("\n")
            if not in_copy:
                if line.startswith("COPY rpo.organization_name_entries"):
                    m = re.search(
                        r"COPY\s+rpo\.organization_name_entries\s*\((.*?)\)\s+FROM", line
                    )
                    if not m:
                        raise RuntimeError(
                            "Nenašiel som zoznam stĺpcov v COPY rpo.organization_name_entries"
                        )
                    col_order = [c.strip().strip('"') for c in m.group(1).split(",")]
                    col_lc = [c.lower() for c in col_order]

                    def idx(name):
                        try:
                            return col_lc.index(name)
                        except ValueError:
                            return None

                    idx_org = idx("organization_id")
                    idx_name = idx("name")
                    idx_eff_from = idx("effective_from")
                    idx_eff_to = idx("effective_to")
                    idx_updated = idx("updated_at")
                    in_copy = True
                    print(f"🧱 rpo.organization_name_entries: {len(col_order)} stĺpcov")
                continue

            if line == r"\.":
                in_copy = False
                continue

            parts = line.split("\t")
            parts = [None if p == r"\N" else p for p in parts]
            if idx_org is None or idx_name is None:
                continue
            org_id = parts[idx_org]
            name = parts[idx_name]
            if not org_id or not name:
                continue

            rec = {
                "name": name,
                "effective_from": parts[idx_eff_from] if idx_eff_from is not None else None,
                "effective_to": parts[idx_eff_to] if idx_eff_to is not None else None,
                "updated_at": parts[idx_updated] if idx_updated is not None else None,
            }
            old = best.get(org_id)
            if _better_name(old, rec):
                best[org_id] = rec

    print(f"🧾 Nájdených názvov: {len(best)} organizácií")
    return best


# --------- parsovanie adries (mesto) ---------

def _better_address(old, new):
    if old is None:
        return True
    if (old.get("effective_to") is None) != (new.get("effective_to") is None):
        return new.get("effective_to") is None
    def ts(x): return x or ""
    if ts(new.get("effective_from")) != ts(old.get("effective_from")):
        return ts(new.get("effective_from")) > ts(old.get("effective_from"))
    if ts(new.get("updated_at")) != ts(old.get("updated_at")):
        return ts(new.get("updated_at")) > ts(old.get("updated_at"))
    return False


def parse_address_map(dump_path: Path):
    print("🔎 Parsujem adresy z rpo.organization_address_entries ...")
    in_copy = False
    col_lc = []
    idx_org = idx_mun = idx_eff_from = idx_eff_to = idx_updated = None
    best = {}

    with gzip.open(dump_path, "rt", encoding="utf-8", newline="") as gz:
        for raw in gz:
            line = raw.rstrip("\n")
            if not in_copy:
                if line.startswith("COPY rpo.organization_address_entries"):
                    m = re.search(
                        r"COPY\s+rpo\.organization_address_entries\s*\((.*?)\)\s+FROM",
                        line,
                    )
                    if not m:
                        raise RuntimeError(
                            "Nenašiel som zoznam stĺpcov v COPY rpo.organization_address_entries"
                        )
                    col_order = [c.strip().strip('"') for c in m.group(1).split(",")]
                    col_lc = [c.lower() for c in col_order]

                    def idx(name):
                        try:
                            return col_lc.index(name)
                        except ValueError:
                            return None

                    idx_org = idx("organization_id")
                    idx_mun = idx("municipality")
                    idx_eff_from = idx("effective_from")
                    idx_eff_to = idx("effective_to")
                    idx_updated = idx("updated_at")
                    in_copy = True
                    print(
                        f"🧱 rpo.organization_address_entries: {len(col_order)} stĺpcov"
                    )
                continue

            if line == r"\.":
                in_copy = False
                continue

            parts = line.split("\t")
            parts = [None if p == r"\N" else p for p in parts]
            if idx_org is None:
                continue
            org_id = parts[idx_org]
            if not org_id:
                continue

            rec = {
                "municipality": parts[idx_mun] if idx_mun is not None else None,
                "effective_from": parts[idx_eff_from] if idx_eff_from is not None else None,
                "effective_to": parts[idx_eff_to] if idx_eff_to is not None else None,
                "updated_at": parts[idx_updated] if idx_updated is not None else None,
            }
            old = best.get(org_id)
            if _better_address(old, rec):
                best[org_id] = rec

    print(f"🧾 Nájdených adries: {len(best)} organizácií")
    return best


# --------- parsovanie IČO ---------

def _better_identifier(old, new):
    if old is None:
        return True
    if (old.get("effective_to") is None) != (new.get("effective_to") is None):
        return new.get("effective_to") is None
    def ts(x): return x or ""
    if ts(new.get("effective_from")) != ts(old.get("effective_from")):
        return ts(new.get("effective_from")) > ts(old.get("effective_from"))
    if ts(new.get("updated_at")) != ts(old.get("updated_at")):
        return ts(new.get("updated_at")) > ts(old.get("updated_at"))
    return False

def parse_identifier_map(dump_path: Path):
    """
    Načíta IČO (IPO) z rpo.organization_identifier_entries.

    Štruktúra tabuľky podľa dokumentácie:
      id, organization_id, ipo, effective_from, effective_to, created_at, updated_at

    ipo používame ako IČO.
    """
    print("🔎 Parsujem IČO z rpo.organization_identifier_entries ...")
    in_copy = False
    col_lc = []
    idx_org = idx_ipo = idx_eff_from = idx_eff_to = idx_updated = None
    best = {}

    with gzip.open(dump_path, "rt", encoding="utf-8", newline="") as gz:
        for raw in gz:
            line = raw.rstrip("\n")

            if not in_copy:
                if line.startswith("COPY rpo.organization_identifier_entries"):
                    m = re.search(
                        r"COPY\s+rpo\.organization_identifier_entries\s*\((.*?)\)\s+FROM",
                        line,
                    )
                    if not m:
                        raise RuntimeError(
                            "Nenašiel som zoznam stĺpcov v COPY rpo.organization_identifier_entries"
                        )
                    col_order = [c.strip().strip('"') for c in m.group(1).split(",")]
                    col_lc = [c.lower() for c in col_order]

                    def idx(name):
                        try:
                            return col_lc.index(name)
                        except ValueError:
                            return None

                    idx_org = idx("organization_id")
                    idx_ipo = idx("ipo")
                    idx_eff_from = idx("effective_from")
                    idx_eff_to = idx("effective_to")
                    idx_updated = idx("updated_at")

                    in_copy = True
                    print(f"🧱 rpo.organization_identifier_entries: {len(col_order)} stĺpcov")
                continue

            if line == r"\.":
                in_copy = False
                continue

            parts = line.split("\t")
            parts = [None if p == r"\N" else p for p in parts]

            if idx_org is None or idx_ipo is None:
                # bez organization_id alebo ipo nemáme čo mapovať
                continue

            org_id = parts[idx_org]
            ipo = parts[idx_ipo]

            if not org_id or not ipo:
                continue

            rec = {
                "ico": ipo,  # tu priamo mapujeme IPO ako IČO
                "effective_from": parts[idx_eff_from] if idx_eff_from is not None else None,
                "effective_to": parts[idx_eff_to] if idx_eff_to is not None else None,
                "updated_at": parts[idx_updated] if idx_updated is not None else None,
            }

            old = best.get(org_id)
            if _better_identifier(old, rec):
                best[org_id] = rec

    print(f"🧾 Nájdených ICO pre {len(best)} organizácií")
    return best


# --------- hlavné parsovanie + SORT podľa created_at ---------
def parse_dump_to_slim_csv(
    dump_path: Path, date_str: str, names_map, addr_map, ident_map, city_region_map
):
    """
    Prečíta rpo.organizations, poskladá finálne riadky do pamäte,
    ZORADÍ ich podľa established_on (najnovšie prvé) a až potom
    ich rozseká do partov:

      snapshots/firms_<date>_part01.csv.gz, part02...

    stĺpce:
      organization_id, ico, name, city, region,
      established_on, terminated_on, last_modified, source_register
    """
    print(f"🔎 Parsujem organizácie z {dump_path} (slim export, sort podľa established_on) ...")
    in_copy = False
    col_order = []
    col_lc = []
    id_idx = est_idx = term_idx = act_idx = created_idx = updated_idx = source_reg_idx = None

    # Bez dátumu v názve – držíme vždy len jeden aktuálny snapshot
    base_name = "firms"
    rows = []

    with gzip.open(dump_path, "rt", encoding="utf-8", newline="") as gz:
        for raw in gz:
            line = raw.rstrip("\n")

            if not in_copy:
                if line.startswith("COPY rpo.organizations"):
                    in_copy = True
                    m = re.search(
                        r"COPY\s+rpo\.organizations\s*\((.*?)\)\s+FROM", line
                    )
                    if not m:
                        raise RuntimeError(
                            "Nenašiel som zoznam stĺpcov v COPY rpo.organizations"
                        )
                    col_order = [c.strip().strip('"') for c in m.group(1).split(",")]
                    col_lc = [c.lower() for c in col_order]

                    def idx(name):
                        try:
                            return col_lc.index(name)
                        except ValueError:
                            return None

                    id_idx = idx("id")
                    est_idx = idx("established_on")
                    term_idx = idx("terminated_on")
                    act_idx = idx("actualized_at")
                    created_idx = idx("created_at")
                    updated_idx = idx("updated_at")
                    source_reg_idx = idx("source_register")

                    print(f"🧱 rpo.organizations má {len(col_order)} stĺpcov")
                continue

            if line == r"\.":
                print("🔚 Koniec COPY rpo.organizations")
                break

            parts = line.split("\t")
            parts = ["" if p == r"\N" else p for p in parts]
            if len(parts) < len(col_order):
                parts += [""] * (len(col_order) - len(parts))

            org_id = parts[id_idx] if id_idx is not None else None
            if not org_id:
                continue

            # názov
            name_info = names_map.get(org_id, {}) or {}
            name_val = name_info.get("name") or ""
            if not name_val:
                for alt in ("business_name", "name", "full_name"):
                    try:
                        ai = col_lc.index(alt)
                    except ValueError:
                        ai = None
                    if ai is not None and parts[ai]:
                        name_val = parts[ai]
                        break

            # mesto
            addr_info = addr_map.get(org_id, {}) or {}
            city = addr_info.get("municipality") or ""

            # kraj
            region = guess_region(city, city_region_map)

            # IČO
            ident_info = ident_map.get(org_id, {}) or {}
            ico = ident_info.get("ico") or ""

            established_on_raw = parts[est_idx] if est_idx is not None else ""
            terminated_on_raw = parts[term_idx] if term_idx is not None else ""

            # last_modified len na informáciu (na stĺpec v CSV), NIE na sort
            candidates = []
            for idx_val in (act_idx, updated_idx, created_idx):
                if idx_val is not None and parts[idx_val]:
                    candidates.append(parts[idx_val])
            if name_info.get("updated_at"):
                candidates.append(name_info["updated_at"])
            if addr_info.get("updated_at"):
                candidates.append(addr_info["updated_at"])
            if ident_info.get("updated_at"):
                candidates.append(ident_info["updated_at"])
            last_modified_raw = max(candidates) if candidates else ""

            established_on_out = _to_dmy(established_on_raw)
            terminated_on_out = _to_dmy(terminated_on_raw)
            last_modified_out = _to_dmy(last_modified_raw)

            created_raw = parts[created_idx] if created_idx is not None else ""

            # 🔑 TERAZ: triedime primárne podľa established_on (novšie prvé),
            # fallback len keď established_on chýba, použijeme created_at.
            sort_key = established_on_raw or created_raw or ""

            row_values = [
                org_id,
                ico,
                name_val,
                city,
                region,
                established_on_out,
                terminated_on_out,
                last_modified_out,
                parts[source_reg_idx] if source_reg_idx is not None else "",
            ]
            rows.append((sort_key, row_values))

    print(f"📊 Načítaných {len(rows)} organizácií, triedim podľa established_on ...")
    # formát je YYYY-MM-DD, takže stringovo triedenie funguje
    rows.sort(key=lambda t: t[0], reverse=True)  # najnovšie založené prvé

    part_index = 0
    part_rows = 0
    out_file = None
    writer = None
    wrote_total = 0

    def start_new_part():
        nonlocal part_index, part_rows, out_file, writer
        if out_file:
            out_file.close()
        part_index += 1
        part_rows = 0
        out_path, out_file, writer = open_part_writer(base_name, part_index)
        header = [
            "organization_id",
            "ico",
            "name",
            "city",
            "region",
            "established_on",
            "terminated_on",
            "last_modified",
            "source_register",
        ]
        writer.writerow(header)
        print(f"📝 píšem do {out_path}")

    for sort_key, row_values in rows:
        if writer is None or part_rows >= ROWS_PER_PART:
            start_new_part()
        writer.writerow(row_values)
        wrote_total += 1
        part_rows += 1
        if wrote_total % 100_000 == 0:
            print(f"  ... {wrote_total} riadkov (part {part_index}, v parte {part_rows})")

    if out_file:
        out_file.close()
    print(f"✅ Uzatvorený posledný part ({part_rows} riadkov)")
    print(f"🎉 Celkovo zapísaných {wrote_total} riadkov, počet partov: {part_index}")
    return wrote_total


def main():
    if not RPO_DUMP_URL:
        raise SystemExit("❌ chýba env RPO_DUMP_URL")

    today_date = datetime.utcnow().date()
    today_dmy = today_date.strftime("%d-%m-%Y")

    # Pred generovaním vyčisti existujúce part súbory (držíme iba jeden snapshot)
    removed = 0
    for p in SNAP_DIR.glob("firms_part*.csv.gz"):
        try:
            p.unlink()
            removed += 1
        except Exception as e:
            print(f"⚠️ Nepodarilo sa zmazať {p}: {e}")
    if removed:
        print(f"🧹 Vymazaných starých partov: {removed}")

    download_dump(RPO_DUMP_URL, TMP_DUMP_PATH)

    city_region_map = load_city_region_map()
    names_map = parse_names_map(TMP_DUMP_PATH)
    addr_map = parse_address_map(TMP_DUMP_PATH)
    ident_map = parse_identifier_map(TMP_DUMP_PATH)

    total = parse_dump_to_slim_csv(
        TMP_DUMP_PATH, today_dmy, names_map, addr_map, ident_map, city_region_map
    )
    print(f"🎉 Hotovo. Spolu {total} riadkov, vytvorených viacero part súborov.")

    # Zapíš posledný dátum aktualizácie (DD-MM-YYYY)
    try:
        (SNAP_DIR / "last_updated.txt").write_text(f"{today_dmy}\n", encoding="utf-8")
        print("🕒 Zapísané snapshots/last_updated.txt")
    except Exception as e:
        print(f"⚠️ Nepodarilo sa zapísať last_updated.txt: {e}")


if __name__ == "__main__":
    main()
