#!/usr/bin/env python3
import sys
import argparse
from pathlib import Path
from typing import Tuple, List, Dict
import pandas as pd


TABLES = ["persone", "documenti", "banche", "fonti", "transazioni"]
PK: Dict[str, List[str]] = {
    "persone":   ["matricola:ID"],
    "documenti": ["id_documento:ID"],
    "banche":    ["id_banca:ID"],
    "fonti":     ["id_fonte:ID"],
    
}


def read_csv_safe(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8", dtype=str, keep_default_na=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp1252", dtype=str, keep_default_na=False)

def require_files(dirpath: Path):
    missing = [t for t in TABLES if not (dirpath / f"{t}.csv").exists()]
    if missing:
        raise FileNotFoundError(f"Nella cartella '{dirpath}' mancano: {', '.join(missing)}")


def norm_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower()

def normalize_all(dfp, dfd, dfb, dff, dft):
    # Persone
    if "matricola:ID" in dfp: dfp["matricola:ID"] = norm_series(dfp["matricola:ID"])
    if "id_banca" in dfp:     dfp["id_banca"]     = norm_series(dfp["id_banca"])
    if "id_documento" in dfp: dfp["id_documento"] = norm_series(dfp["id_documento"])
    if "id_fonte" in dfp:     dfp["id_fonte"]     = norm_series(dfp["id_fonte"])
    # Documenti
    if "id_documento:ID" in dfd: dfd["id_documento:ID"] = norm_series(dfd["id_documento:ID"])
    if "matricola" in dfd:       dfd["matricola"]       = norm_series(dfd["matricola"])
    # Banche
    if "id_banca:ID" in dfb:     dfb["id_banca:ID"]     = norm_series(dfb["id_banca:ID"])
    # Fonti
    if "id_fonte:ID" in dff:     dff["id_fonte:ID"]     = norm_series(dff["id_fonte:ID"])
    # Transazioni
    if "matricola" in dft:       dft["matricola"]       = norm_series(dft["matricola"])
    if "destinatario" in dft:    dft["destinatario"]    = norm_series(dft["destinatario"])
    if "id_banca_deriva" in dft: dft["id_banca_deriva"] = norm_series(dft["id_banca_deriva"])
    return dfp, dfd, dfb, dff, dft

def load_subset_dir(dirpath: Path):
    dfp = read_csv_safe(dirpath / "persone.csv")
    dfd = read_csv_safe(dirpath / "documenti.csv")
    dfb = read_csv_safe(dirpath / "banche.csv")
    dff = read_csv_safe(dirpath / "fonti.csv")
    dft = read_csv_safe(dirpath / "transazioni.csv")
    return normalize_all(dfp, dfd, dfb, dff, dft)


def assert_keys_subset(df_small: pd.DataFrame, df_big: pd.DataFrame, keycols: List[str], label: str):
    for col in keycols:
        if col not in df_small.columns or col not in df_big.columns:
            raise AssertionError(f"[{label}] colonna chiave mancante per il confronto: {col}")

    small_keys = set(map(tuple, df_small[keycols].values.tolist()))
    big_keys   = set(map(tuple, df_big[keycols].values.tolist()))
    missing = small_keys - big_keys
    if missing:
        example = next(iter(missing))
        raise AssertionError(f"[{label}] annidamento chiavi violato: {len(missing)} chiavi mancanti (esempio: {example})")

def assert_rows_subset(df_small: pd.DataFrame, df_big: pd.DataFrame, label: str):
    common = [c for c in df_small.columns if c in df_big.columns]
    if not common:
        raise AssertionError(f"[{label}] nessuna colonna in comune per il confronto righe.")
    a = df_small[common].drop_duplicates()
    b = df_big[common].drop_duplicates()
    merged = a.merge(b, how="left", on=common, indicator=True)
    missing_rows_mask = merged["_merge"] == "left_only"
    missing_count = int(missing_rows_mask.sum())
    if missing_count:
        sample = a[missing_rows_mask].head(1).to_dict(orient="records")[0]
        raise AssertionError(f"[{label}] annidamento righe violato: {missing_count} righe non trovate. Esempio: {sample}")

def verify_nested(small_dir: Path, big_dir: Path, skip_keys: bool=False, skip_rows: bool=False) -> None:
    require_files(small_dir)
    require_files(big_dir)

    s_p, s_d, s_b, s_f, s_t = load_subset_dir(small_dir)
    b_p, b_d, b_b, b_f, b_t = load_subset_dir(big_dir)

    print(f"Confronto: {small_dir.name} ⊂ {big_dir.name}")
    print("---- Riepilogo righe ----")
    print(f"persone       {len(s_p):>8} ⊂ {len(b_p):<8}")
    print(f"documenti     {len(s_d):>8} ⊂ {len(b_d):<8}")
    print(f"banche        {len(s_b):>8} ⊂ {len(b_b):<8}")
    print(f"fonti         {len(s_f):>8} ⊂ {len(b_f):<8}")
    print(f"transazioni   {len(s_t):>8} ⊂ {len(b_t):<8}")
    print("-------------------------")

    
    if not skip_keys:
        assert_keys_subset(s_p, b_p, PK["persone"],   "persone")
        assert_keys_subset(s_d, b_d, PK["documenti"], "documenti")
        assert_keys_subset(s_b, b_b, PK["banche"],    "banche")
        assert_keys_subset(s_f, b_f, PK["fonti"],     "fonti")
        print("OK chiavi: persone, documenti, banche, fonti")

    
    if not skip_rows:
        assert_rows_subset(s_p, b_p, "persone")
        assert_rows_subset(s_d, b_d, "documenti")
        assert_rows_subset(s_b, b_b, "banche")
        assert_rows_subset(s_f, b_f, "fonti")
        assert_rows_subset(s_t, b_t, "transazioni")
        print("OK righe: persone, documenti, banche, fonti, transazioni")

    print("il subset è contenuto nel più grande.")


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Verifica che una cartella subset (piccola) sia sottoinsieme di un'altra (grande)."
    )
    p.add_argument("small_dir", type=Path, help="Directory del subset più piccolo (es. subset_25)")
    p.add_argument("big_dir",   type=Path, help="Directory del subset più grande (es. subset_50)")
    p.add_argument("--skip-keys", action="store_true", help="Salta il controllo di sottoinsieme sulle chiavi PK.")
    p.add_argument("--skip-rows", action="store_true", help="Salta il controllo riga-per-riga (colonne comuni).")
    return p.parse_args(argv)

def main(argv: List[str]) -> int:
    args = parse_args(argv)
    try:
        verify_nested(args.small_dir, args.big_dir, skip_keys=args.skip_keys, skip_rows=args.skip_rows)
        return 0
    except AssertionError as e:
        print(f" Errore: {e}")
        return 2
    except FileNotFoundError as e:
        print(f"File mancante: {e}")
        return 3
    except Exception as e:
        print(f"Errore inatteso: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
