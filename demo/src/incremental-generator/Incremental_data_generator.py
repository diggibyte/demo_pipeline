# Databricks notebook source
import numpy as np
import pandas as pd
import io
from datetime import date, timedelta, datetime
from pathlib import Path
 

 
CATALOG = "chalmers_demo"             
SCHEMA  = "demo_data"               
VOLUME  = "sap_data"         
 

CUSTOMERS_PER_RUN = 500       # new customer rows each run
MATERIALS_PER_RUN = 800       # new material rows each run
SO_ORDERS_PER_RUN = 3_000     # new sales orders (each has 1–5 line items)
 

 
VOLUME_ROOT    = Path(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}")
CUST_DIR       = VOLUME_ROOT / "customer"
MAT_DIR        = VOLUME_ROOT / "material"
SO_DIR         = VOLUME_ROOT / "Sales"
MAX_FILE_BYTES = 10 * 1024 * 1024   # 10 MB hard cap per file
 

 
LAND1    = ['GB','AE','SE','HK','ES','SG','NL','IN','CH','CA',
            'DK','DE','NO','US','PL','FI','AU','BR','JP','FR']
BRSCH    = ['TEXT','FOOD','AGRI','ELEC','MECH','PHARMA','CHEM','AUTO','MINE','ENRG']
KTOKD    = ['ZDIS','ZINT','ZDOM','ZAGN','ZEXP']
DIRTY    = ['  ', 'N/A', 'NULL', 'null', '#N/A', '']   # ~5% noise
 
MTART    = ['HIBE','DIEN','VERP','ROH','ERSA','FERT','HALB']
MATKL    = [f'MG{i:03d}' for i in range(1, 9)]
MEINS    = ['PC','M3','M','KG','EA','L','BOX','M2','SET','BAG']
 
AUART    = ['ZOR','ZQT','ZRE','ZCO','ZUB']
WERKS    = ['P001','P002','P003','P004','P005']
VKORG    = ['SO01','SO02','SO03','SO04']
BUKRS    = [1000, 2000, 3000, 4000]
WAERK    = ['AED','AUD','BRL','CAD','CHF','EUR','GBP','HKD','INR','JPY','SEK','SGD','USD']
 
SURNAMES = [
    'Rodriguez','Figueroa','Sanchez','Doyle','Mcclain','Miller','Henderson',
    'Davis','Guzman','Hoffman','Baldwin','Gardner','Robinson','Lawrence',
    'Blake','Ramirez','Lewis','Garcia','Abbott','Munoz','Johnson','Williams',
    'Brown','Jones','Martin','Thompson','White','Harris','Clark','Walker',
    'Hall','Allen','Young','King','Wright','Scott','Green','Adams','Baker',
    'Nelson','Carter','Mitchell','Perez','Roberts','Turner','Phillips','Campbell'
]
SUFFIXES = ['Ltd','LLC','Inc','Corp','Group','and Sons','& Co','and Partners']
 
MAT_ADJ  = ['Ultra','Precision','Pro','Heavy-Duty','Standard','Advanced',
            'Compact','Industrial','Modular','Flex','Smart','Micro','Light']
MAT_NOUN = ['Sensor','Nut','Connector','Gear','Valve','Bearing','Pump',
            'Washer','Motor','Coupling','Bolt','Plate','Cable','Spring',
            'Shaft','Bracket','Filter','Relay','Switch','Flange','Bushing']
 

 
def get_last_id(directory: Path, id_col: str, prefix: str) -> int:
    """
    Scans all CSVs in `directory`, reads the `id_col` column, strips the
    `prefix`, and returns the highest integer ID found. Returns 0 if no
    files exist yet (first run).
 
    Example: prefix='CUST', id_col='KUNNR' → reads 'CUST0008500' → 8500
    """
    csv_files = sorted(directory.glob("*.csv"))
    if not csv_files:
        print(f"   No existing files in {directory.name}/ — starting from ID 1")
        return 0
 
    max_id = 0
    for f in csv_files:
        try:
            # Read only the ID column for speed
            df_ids = pd.read_csv(f, usecols=[id_col], dtype=str)
            ids = (
                df_ids[id_col]
                .dropna()
                .str.replace(prefix, "", regex=False)
                .str.strip()
            )
            numeric_ids = pd.to_numeric(ids, errors="coerce").dropna().astype(int)
            if not numeric_ids.empty:
                max_id = max(max_id, int(numeric_ids.max()))
        except Exception as e:
            print(f"   Could not read {f.name}: {e}")
 
    print(f"   {directory.name}/ — last {id_col}: {prefix}{max_id:>09}  ({len(csv_files)} file(s) found)")
    return max_id
 

#  HELPER FUNCTIONS

 
def _dates(rng, n: int, yr_start: int = 2010, yr_end: int = 2024) -> list:
    """Generate n random ISO date strings between yr_start and yr_end."""
    base  = date(yr_start, 1, 1)
    delta = (date(yr_end, 12, 31) - base).days
    return [(base + timedelta(days=int(d))).isoformat()
            for d in rng.integers(0, delta, n)]
 
 
def _inject_dirty(rng, arr, rate: float = 0.05) -> np.ndarray:
    """Randomly replace ~rate fraction of values with dirty/null tokens."""
    arr  = np.array(arr, dtype=object)
    mask = rng.random(len(arr)) < rate
    if mask.any():
        arr[mask] = rng.choice(DIRTY, int(mask.sum()))
    return arr
 
 
def _cap_df(df: pd.DataFrame, label: str, max_bytes: int = MAX_FILE_BYTES) -> pd.DataFrame:
    """Trim DataFrame rows so the CSV stays under max_bytes (10 MB)."""
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    raw = buf.getvalue().encode()
    if len(raw) <= max_bytes:
        return df
    bytes_per_row = len(raw) / (len(df) + 1)
    keep = max(1, int(max_bytes / bytes_per_row) - 1)
    print(f"   [{label}] Capped at 10 MB — trimmed from {len(df):,} to {keep:,} rows")
    return df.iloc[:keep].reset_index(drop=True)
 

#  GENERATOR FUNCTIONS

def gen_customers(rng, start_id: int, n: int) -> pd.DataFrame:
    """
    Generate n customer rows starting from start_id + 1.
    Columns: KUNNR · NAME1 · LAND1 · BRSCH · KTOKD · CREDIT_LIMIT · ERDAT · AEDAT
    """
    ids   = np.arange(start_id + 1, start_id + n + 1)
    kunnr = [f"CUST{i:07d}" for i in ids]
 
    s1, s2, s3 = (rng.choice(SURNAMES, n) for _ in range(3))
    suf   = rng.choice(SUFFIXES, n)
    style = rng.integers(0, 4, n)
    names = []
    for j in range(n):
        if   style[j] == 0: names.append(f"{s1[j]} {suf[j]}")
        elif style[j] == 1: names.append(f"{s1[j]} and {s2[j]}")
        elif style[j] == 2: names.append(f"{s1[j]}, {s2[j]} and {s3[j]}")
        else:                names.append(f"{s1[j]}-{s2[j]}")
 
    erdat = _dates(rng, n, 2010, 2022)
    aedat = [(date.fromisoformat(e) + timedelta(days=int(rng.integers(30, 900)))).isoformat()
             for e in erdat]
 
    df = pd.DataFrame({
        "KUNNR"        : kunnr,
        "NAME1"        : names,
        "LAND1"        : _inject_dirty(rng, rng.choice(LAND1, n)),
        "BRSCH"        : _inject_dirty(rng, rng.choice(BRSCH, n)),
        "KTOKD"        : rng.choice(KTOKD, n),
        "CREDIT_LIMIT" : np.round(rng.uniform(51_221.27, 4_999_654.42, n), 2),
        "ERDAT"        : erdat,
        "AEDAT"        : aedat,
    })
    return _cap_df(df, "customers")
 
 
def gen_materials(rng, start_id: int, n: int) -> pd.DataFrame:
    """
    Generate n material rows starting from start_id + 1.
    Columns: MATNR · MAKTX · MTART · MATKL · MEINS · STANDARD_PRICE · BRGEW · ERDAT · AEDAT
    """
    ids   = np.arange(start_id + 1, start_id + n + 1)
    matnr = [f"MAT{i:08d}" for i in ids]
 
    adj, noun, num = rng.choice(MAT_ADJ, n), rng.choice(MAT_NOUN, n), rng.integers(1, 1000, n)
    maktx = [f"{adj[j]} {noun[j]} {num[j]:03d}" for j in range(n)]
 
    erdat = _dates(rng, n, 2010, 2022)
    aedat = [(date.fromisoformat(e) + timedelta(days=int(rng.integers(30, 900)))).isoformat()
             for e in erdat]
 
    df = pd.DataFrame({
        "MATNR"          : matnr,
        "MAKTX"          : maktx,
        "MTART"          : _inject_dirty(rng, rng.choice(MTART, n)),
        "MATKL"          : rng.choice(MATKL, n),
        "MEINS"          : rng.choice(MEINS, n),
        "STANDARD_PRICE" : np.round(rng.uniform(1.73, 49_998.30, n), 2),
        "BRGEW"          : np.round(rng.uniform(0.319, 4_999.879, n), 3),
        "ERDAT"          : erdat,
        "AEDAT"          : aedat,
    })
    return _cap_df(df, "materials")
 
 
def gen_sales_orders(rng, so_start_id: int, n_orders: int,
                     cust_id_end: int, mat_id_end: int) -> pd.DataFrame:
    """
    Generate sales order line items starting from so_start_id + 1.
    Each VBELN gets 1–5 POSNR line items (mirrors source CSV pattern).
    KUNNR / MATNR are drawn from IDs available so far (realistic FK refs).
 
    Columns: VBELN · POSNR · AUART · KUNNR · MATNR · WERKS · VKORG · BUKRS ·
             WAERK · KWMENG · NETWR · NETPR · DISCOUNT_PCT · TAX_AMOUNT · ERDAT
    """
    cust_pool = [f"CUST{i:07d}"
                 for i in rng.integers(1, max(2, cust_id_end + 1),
                                       min(500, cust_id_end + 1))]
    mat_pool  = [f"MAT{i:08d}"
                 for i in rng.integers(1, max(2, mat_id_end + 1),
                                       min(500, mat_id_end + 1))]
    rows  = []
    so_id = so_start_id + 1
 
    for _ in range(n_orders):
        vbeln   = f"SO{so_id:09d}"
        n_items = int(rng.integers(1, 6))
        erdat_v = _dates(rng, 1, 2015, 2024)[0]
 
        for pos in range(1, n_items + 1):
            kwmeng = round(float(rng.uniform(1.001, 999.998)), 3)
            netpr  = round(float(rng.uniform(5.25, 49_999.63)), 2)
            disc   = round(float(rng.uniform(0.0, 30.0)), 2)
            netwr  = round(min(kwmeng * netpr, 9_999_999.99), 2)
            tax    = round(netwr * round(float(rng.uniform(0.05, 0.30)), 3), 2)
 
            rows.append({
                "VBELN"        : vbeln,
                "POSNR"        : pos * 10,
                "AUART"        : str(rng.choice(AUART)),
                "KUNNR"        : str(rng.choice(cust_pool)),
                "MATNR"        : str(rng.choice(mat_pool)),
                "WERKS"        : str(rng.choice(WERKS)),
                "VKORG"        : str(rng.choice(VKORG)),
                "BUKRS"        : int(rng.choice(BUKRS)),
                "WAERK"        : str(rng.choice(WAERK)),
                "KWMENG"       : kwmeng,
                "NETWR"        : netwr,
                "NETPR"        : netpr,
                "DISCOUNT_PCT" : disc,
                "TAX_AMOUNT"   : tax,
                "ERDAT"        : erdat_v,
            })
        so_id += 1
 
    df = pd.DataFrame(rows)
    df["POSNR"] = df["POSNR"].astype(int).apply(lambda x: f"{x:06d}")
    return _cap_df(df, "sales_orders")
 

#  MAIN PIPELINE

def run_pipeline():
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    rng    = np.random.default_rng(seed=int(datetime.now().timestamp()))
 
    print("=" * 65)
    print(f" PIPELINE RUN  ▶  {run_ts}")
    print("=" * 65)
 
    # Create Volume directories if they don't exist 
    for d in [CUST_DIR, MAT_DIR, SO_DIR]:
        d.mkdir(parents=True, exist_ok=True)
 
    #  Detect last used IDs by scanning existing files
    print("\n Scanning existing files to detect last used IDs…")
    last_cust_id = get_last_id(CUST_DIR, id_col="KUNNR", prefix="CUST")
    last_mat_id  = get_last_id(MAT_DIR,  id_col="MATNR", prefix="MAT")
    last_so_id   = get_last_id(SO_DIR,   id_col="VBELN", prefix="SO")
 
    #  Generate incremental data 
    print("\n Generating incremental records…")
 
    df_cust = gen_customers(rng, last_cust_id, CUSTOMERS_PER_RUN)
    df_mat  = gen_materials(rng, last_mat_id,  MATERIALS_PER_RUN)
    df_so   = gen_sales_orders(
                  rng,
                  so_start_id  = last_so_id,
                  n_orders     = SO_ORDERS_PER_RUN,
                  cust_id_end  = last_cust_id + len(df_cust),
                  mat_id_end   = last_mat_id  + len(df_mat),
              )
 
    #  Write CSVs to Volume directories 
    print("\n  Writing CSVs to Databricks Volume…")
 
    cust_file = CUST_DIR / f"customers_incr_{run_ts}.csv"
    mat_file  = MAT_DIR  / f"materials_incr_{run_ts}.csv"
    so_file   = SO_DIR   / f"sales_orders_incr_{run_ts}.csv"
 
    df_cust.to_csv(cust_file, index=False)
    df_mat.to_csv(mat_file,   index=False)
    df_so.to_csv(so_file,     index=False)
 
    #  Print summary 
    print()
    for label, fpath, df in [
        ("customers",    cust_file, df_cust),
        ("materials",    mat_file,  df_mat),
        ("sales_orders", so_file,   df_so),
    ]:
        mb     = fpath.stat().st_size / 1024 / 1024
        status = "" if mb <= 10 else " EXCEEDS 10 MB"
        print(f"   {status}  {label:<15}  {len(df):>7,} rows  {mb:.3f} MB")
        print(f"            → {fpath.name}")
 
    new_cust_id = last_cust_id + len(df_cust)
    new_mat_id  = last_mat_id  + len(df_mat)
    new_so_id   = last_so_id   + SO_ORDERS_PER_RUN
 
    print(f"\n Run complete!")
    print(f"   Generated KUNNR : CUST{last_cust_id + 1:07d}  →  CUST{new_cust_id:07d}")
    print(f"   Generated MATNR : MAT{last_mat_id  + 1:08d}  →  MAT{new_mat_id:08d}")
    print(f"   Generated VBELN : SO{last_so_id    + 1:09d}  →  SO{new_so_id:09d}")
    print(f"\n   Next run will continue from:")
    print(f"   KUNNR → CUST{new_cust_id + 1:07d}")
    print(f"   MATNR → MAT{new_mat_id  + 1:08d}")
    print(f"   VBELN → SO{new_so_id    + 1:09d}")
 
 
if __name__ == "__main__":
    run_pipeline()