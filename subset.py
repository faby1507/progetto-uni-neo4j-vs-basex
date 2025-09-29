import pandas as pd
from pathlib import Path
from typing import Tuple


#la percentuale viene applicata solo alle persone, tutto il resto viene “portato dietro” in modo coerente

#conf
INPUT_DIR   = Path(".")     #cartella CSV originali
OUTPUT_ROOT = Path(".")  #dove creare subset_25/, subset_50/, subset_75/
RANDOM_SEED = 42
PERCENTS    = [0.25, 0.50, 0.75] #percentuali 
MODE        = "any"         

#lettura dei csv 
def read_csv_safe(path: Path) -> pd.DataFrame: #Definisce una funzione che prende in input un percorso di file (Path) e restituisce un DataFrame pandas
    try:
        return pd.read_csv(path, encoding="utf-8", dtype=str, keep_default_na=False) #se la lettura da errore riprova con un altrof ormato 
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp1252", dtype=str, keep_default_na=False)

def write_csv(df: pd.DataFrame, path: Path): #Definisce una funzione che salva un dataframe in CSV, creando prima le cartelle se mancano
    path.parent.mkdir(parents=True, exist_ok=True) #crea la cartella corrispondente a quel Path. se le cartelle superiori non esistono, le crea tutte
    df.to_csv(path, index=False, encoding="utf-8") #non salva l'indice come colonna 

#Normalizzazione
def norm_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower() #rende tutto stringa minuscola senza spazi 

def normalize_all(dfp, dfd, dfb, dff, dft):
    #Persone
    dfp["matricola:ID"] = norm_series(dfp["matricola:ID"])
    dfp["id_banca"]     = norm_series(dfp["id_banca"])
    dfp["id_documento"] = norm_series(dfp["id_documento"])
    dfp["id_fonte"]     = norm_series(dfp["id_fonte"])
    #Documenti
    dfd["id_documento:ID"] = norm_series(dfd["id_documento:ID"])
    dfd["matricola"]       = norm_series(dfd["matricola"])
    #Banche
    dfb["id_banca:ID"]     = norm_series(dfb["id_banca:ID"])
    #Fonti
    dff["id_fonte:ID"]     = norm_series(dff["id_fonte:ID"])
    #Transazioni
    dft["matricola"]       = norm_series(dft["matricola"])        # mittente
    dft["destinatario"]    = norm_series(dft["destinatario"])     # destinatario
    if "id_banca_deriva" in dft.columns: #Controlla se il DataFrame dft ha la colonna id_banca_deriva e, solo se esiste, la normalizza
        dft["id_banca_deriva"] = norm_series(dft["id_banca_deriva"])
    return dfp, dfd, dfb, dff, dft

#caricamento 
def load_all(input_dir: Path):
    df_persone     = read_csv_safe(input_dir / "persone.csv")
    df_documenti   = read_csv_safe(input_dir / "documenti.csv")
    df_banche      = read_csv_safe(input_dir / "banche.csv")
    df_fonti       = read_csv_safe(input_dir / "fonti.csv")
    df_transazioni = read_csv_safe(input_dir / "transazioni.csv")
    return normalize_all(df_persone, df_documenti, df_banche, df_fonti, df_transazioni)

#filtro transazioni. filtra il DataFrame delle transazioni in base a un insieme di persone (people_ids) e a una modalità
def transazioni_subset(df_t: pd.DataFrame, people_ids: set, mode: str) -> pd.DataFrame:
    if mode == "any": #più completo, include tutte le transazioni in cui almeno una delle due persone (mittente o destinatario) è nel subset.
        mask = df_t["matricola"].isin(people_ids) | df_t["destinatario"].isin(people_ids) #mittente OPPURE destinatario è in people_ids
    elif mode == "both":
        mask = df_t["matricola"].isin(people_ids) & df_t["destinatario"].isin(people_ids) #sia mittente sia destinatario sono in people_ids. riduce num trans
    elif mode == "dest":
        mask = df_t["destinatario"].isin(people_ids) #solo il destinatario è in people_ids
    elif mode == "src":
        mask = df_t["matricola"].isin(people_ids) #solo il mittente è in people_ids
    else:
        raise ValueError("MODE deve essere: any | both | dest | src")
    return df_t[mask].copy() #restituisce un nuovo DataFrame

#riorganizza colonne
def reorder_like(df_sub: pd.DataFrame, df_full: pd.DataFrame) -> pd.DataFrame: #restituisce df nuovo riordinato prendendo in ingresso quello minore e quello più grande 
    cols = [c for c in df_full.columns if c in df_sub.columns] #contiene le colonne di df_full che sono anche presenti in df_sub, rispettando l’ordine di df_full.
    extra = [c for c in df_sub.columns if c not in cols] #contiene invece le colonne di df_sub che non sono state trovate in df_full, quindi quelle “in più” rispetto a df_full
    return df_sub[cols + extra] #Se una colonna è in entrambi, viene messa nell’ordine di df_full


def build_orders(dfp: pd.DataFrame, dfb: pd.DataFrame, dff: pd.DataFrame, dft: pd.DataFrame):
    people_order = dfp["matricola:ID"].tolist() #prende la colonna matricola:ID dal DataFrame delle persone (dfp).
#risulta in una pd.Series con tutti gli ID delle persone, nell’ordine in cui compaiono nel CSV.
#.tolist() converte quella Series in una lista Python.
    used_banks_full = set(dfp["id_banca"]) #prende dal DataFrame delle persone (dfp) la colonna id_banca, cioè la banca associata a ciascuna persona. Risultato: una pd.Series.
#set(...) converte quella serie in un insieme Python (set).
    if "id_banca_deriva" in dft.columns:
         #allora aggiorna l’insieme delle banche usate (used_banks_full)
    #aggiungendo tutti i valori presenti in "id_banca_deriva"
    #MA solo se sono stringhe e non vuote ("")
        used_banks_full |= {x for x in dft["id_banca_deriva"] if isinstance(x, str) and x != ""}
        #Seleziona dal DataFrame delle banche (dfb) solo quelle NON presenti in used_banks_full
#cioè le banche che non risultano usate né da persone né da transazioni.

    dfb_orphans_full = dfb[~dfb["id_banca:ID"].isin(used_banks_full)]
    #converte la colonna "id_banca:ID" delle banche orfane in una lista Python
    orphan_banks_order = dfb_orphans_full["id_banca:ID"].tolist()
    #Crea un insieme con tutte le fonti (id_fonte) usate dalle persone
    used_sources_full = set(dfp["id_fonte"])
    #Seleziona dal DataFrame delle fonti (dff) solo quelle NON usate da nessuna persona
    dff_orphans_full = dff[~dff["id_fonte:ID"].isin(used_sources_full)]
    #Converte la colonna "id_fonte:ID" delle fonti orfane in una lista Python
    orphan_sources_order = dff_orphans_full["id_fonte:ID"].tolist()
    return people_order, orphan_banks_order, orphan_sources_order

#creazione subset
def make_subset(
    dfp: pd.DataFrame, dfd: pd.DataFrame, dfb: pd.DataFrame, dff: pd.DataFrame, dft: pd.DataFrame,
    fraction: float, mode: str,
    people_order, orphan_banks_order, orphan_sources_order
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    #PERSONE
    k_people = int(round(len(dfp) * fraction)) #calcola quante persone mettere nel subset in base alla frazione richiesta.
    people_ids = set(people_order[:k_people]) #lo converte in un set 
    dfp_sub = dfp[dfp["matricola:ID"].isin(people_ids)].copy() #copia gli id delle persone nel subset 

    #DOCUMENTI delle persone
    dfd_sub = dfd[dfd["matricola"].isin(people_ids)].copy() #copia i documenti associati alle matricole 

    #TRANSAZIONI 
    dft_sub = transazioni_subset(dft, people_ids, mode)

    #BANCHE usate da persone/transazion
    bank_ids_people = set(dfp_sub["id_banca"]) #crea set con id delle banche 
    bank_ids_tx = set(dft_sub["id_banca_deriva"]) if "id_banca_deriva" in dft_sub.columns else set()
    bank_ids_all = bank_ids_people | {x for x in bank_ids_tx if isinstance(x, str) and x != ""} #bank_ids_all contiene gli ID banca da persone + da transazioni
    dfb_sub = dfb[dfb["id_banca:ID"].isin(bank_ids_all)].copy() 

    #+ ORfANE
    k_banks = int(round(len(orphan_banks_order) * fraction)) #numero di banche orfane 
    if k_banks > 0:
        orphan_bank_ids = set(orphan_banks_order[:k_banks]) #crea set di banche orfane in base alla frazione
        dfb_sub = (pd.concat([dfb_sub, dfb[dfb["id_banca:ID"].isin(orphan_bank_ids)]], ignore_index=True)
                     .drop_duplicates(subset=["id_banca:ID"], keep="first")) #Aggiunge le banche orfane a dfb_sub e poi rimuove eventuali duplicati sull’ID banca

    #FONTI referenziate dalle persone
    fonte_ids = set(dfp_sub["id_fonte"])
    dff_sub = dff[dff["id_fonte:ID"].isin(fonte_ids)].copy()

    # + ORFANE
    k_sources = int(round(len(orphan_sources_order) * fraction)) #numero di fonti del subset  
    if k_sources > 0:
        orphan_source_ids = set(orphan_sources_order[:k_sources])
        dff_sub = (pd.concat([dff_sub, dff[dff["id_fonte:ID"].isin(orphan_source_ids)]], ignore_index=True)
                     .drop_duplicates(subset=["id_fonte:ID"], keep="first"))

    #riordino colonne come negli originali
    dfp_sub  = reorder_like(dfp_sub, dfp)
    dfd_sub  = reorder_like(dfd_sub, dfd)
    dfb_sub  = reorder_like(dfb_sub, dfb)
    dff_sub  = reorder_like(dff_sub, dff)
    dft_sub  = reorder_like(dft_sub, dft)

    return dfp_sub, dfd_sub, dfb_sub, dff_sub, dft_sub


#Salvataggio
def save_subset(out_dir: Path, dfs: Tuple[pd.DataFrame, ...]):
    dfp, dfd, dfb, dff, dft = dfs
    write_csv(dfp, out_dir / "persone.csv")
    write_csv(dfd, out_dir / "documenti.csv")
    write_csv(dfb, out_dir / "banche.csv")
    write_csv(dff, out_dir / "fonti.csv")
    write_csv(dft, out_dir / "transazioni.csv")

#Main
def main():
    dfp, dfd, dfb, dff, dft = load_all(INPUT_DIR)
    people_order, orphan_banks_order, orphan_sources_order = build_orders(dfp, dfb, dff, dft)

    for frac in PERCENTS:
        name = f"{int(frac*100)}"
        out_dir = OUTPUT_ROOT / f"subset_{name}"

        dfs = make_subset(
            dfp, dfd, dfb, dff, dft,
            fraction=frac, mode=MODE,
            people_order=people_order,
            orphan_banks_order=orphan_banks_order,
            orphan_sources_order=orphan_sources_order
        )

       
        save_subset(out_dir, dfs)
        print(f"Subset {name}% salvato in {out_dir}")
        print(f"  persone: {len(dfs[0])}  documenti: {len(dfs[1])}  banche: {len(dfs[2])}  fonti: {len(dfs[3])}  transazioni: {len(dfs[4])}")

if __name__ == "__main__":
    main()
