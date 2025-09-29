
import csv
import os
from xml.sax.saxutils import escape

#Config 
INPUT_DIR = "."                #cartella CSV
OUTPUT_FILE = "graph.xml"      #XML risultante
WRITE_RELATIONS = False        #True per aggiungere la sezione <Relazioni>

#utility 
def esc(v): return "" if v is None else escape(str(v))

def open_csv(path):
    #prova ad aprire un CSV in UTF-8 e, se fallisce lo riapre in CP1252
    try:
        return open(path, encoding="utf-8", newline="") 
    except UnicodeDecodeError:
        return open(path, encoding="cp1252", newline="")

def write_open(f, tag):  f.write(f"<{tag}>\n") #per scrivere tag XML su un file già aperto in modalità testo
def write_close(f, tag): f.write(f"</{tag}>\n")


def load_banche(): #legge banche.csv e costruisce un dizionario di banche indicizzato per ID
    seen = {} #dizionario vuoto per accumulare le banche
    with open_csv(os.path.join(INPUT_DIR, "banche.csv")) as fin: #apre il file 'banche.csv' dentro INPUT_DIR usando open_csv 
        for row in csv.DictReader(fin):
            bid = row.get("id_banca:ID") #Legge il CSV riga per riga come dizionari ed estrae l'ID banca dalla colonna 'id_banca:ID'
            if bid and bid not in seen: #se l'ID esiste ed è nuovo (non già incontrato), allora registra la banca.
                seen[bid] = { #crea l'entry nel dizionario indicizzato per ID
                    "id": bid,
                    "nome": row.get("nome"),
                    "nazione": row.get("nazione"),
                    "max_deposito": row.get("max_deposito:INT"),
                }
    return seen

def load_fonti(): #idem
    seen = {}
    with open_csv(os.path.join(INPUT_DIR, "fonti.csv")) as fin:
        for row in csv.DictReader(fin):
            fid = row.get("id_fonte:ID")
            if fid and fid not in seen:
                seen[fid] = {
                    "id": fid,
                    "nome": row.get("nome"),
                    "nazione": row.get("nazione"),
                    "affidabilita": row.get("affidabilita:FLOAT"),
                }
    return seen

def main():
    banche = load_banche()
    fonti  = load_fonti()

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fout:
        #header
        fout.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        fout.write("<Graph>\n") #scrive l'inizio del file xml 
        write_open(fout, "Nodi") 

        #Persone
        write_open(fout, "Persone")
        with open_csv(os.path.join(INPUT_DIR, "persone.csv")) as fin:
            r = csv.DictReader(fin)
            for row in r:
                mid = row.get("matricola:ID")
                if not mid: 
                    continue
                fout.write(
                    '  <Persona '
                    f'matricola="{esc(mid)}" '
                    f'stipendio="{esc(row.get("stipendio:INT"))}">\n'
                )
                fout.write(f'    <Nome>{esc(row.get("nome"))}</Nome>\n')
                fout.write(f'    <Cognome>{esc(row.get("cognome"))}</Cognome>\n')
                fout.write(f'    <BancaRef id="{esc(row.get("id_banca"))}"/>\n')
                fout.write(f'    <DocumentoRef id="{esc(row.get("id_documento"))}"/>\n')
                fid = row.get("id_fonte") or row.get("id_fonte:ID")
                fout.write(f'    <FonteRef id="{esc(fid)}"/>\n')
                fout.write("  </Persona>\n")
        write_close(fout, "Persone")

        #Documenti
        write_open(fout, "Documenti")
        with open_csv(os.path.join(INPUT_DIR, "documenti.csv")) as fin:
            r = csv.DictReader(fin)
            for row in r:
                did = row.get("id_documento:ID")
                if not did: 
                    continue
                fout.write(
                    '  <Documento '
                    f'id="{esc(did)}" '
                    f'nazione="{esc(row.get("nazione"))}" '
                    f'scadenza="{esc(row.get("scadenza"))}">\n'
                )
                fout.write(f'    <Email>{esc(row.get("email"))}</Email>\n')
                fout.write(f'    <NumeroTelefono>{esc(row.get("num_telefono"))}</NumeroTelefono>\n')
                fout.write(f'    <PersonaRef matricola="{esc(row.get("matricola"))}"/>\n')
                fout.write("  </Documento>\n")
        write_close(fout, "Documenti")

        #Banche
        write_open(fout, "Banche")
        for b in banche.values():
            fout.write(
                '  <Banca '
                f'id="{esc(b["id"])}" '
                f'nazione="{esc(b.get("nazione"))}" '
                f'max_deposito="{esc(b.get("max_deposito"))}">\n'
            )
            fout.write(f'    <Nome>{esc(b.get("nome"))}</Nome>\n')
            fout.write("  </Banca>\n")
        write_close(fout, "Banche")

        #Fonti
        write_open(fout, "Fonti")
        for f in fonti.values():
            fout.write(
                '  <Fonte '
                f'id="{esc(f["id"])}" '
                f'nazione="{esc(f.get("nazione"))}" '
                f'affidabilita="{esc(f.get("affidabilita"))}">\n'
            )
            fout.write(f'    <Nome>{esc(f.get("nome"))}</Nome>\n')
            fout.write("  </Fonte>\n")
        write_close(fout, "Fonti")

        #Transazioni (con BancaDerivaRef)
        write_open(fout, "Transazioni")
        with open_csv(os.path.join(INPUT_DIR, "transazioni.csv")) as fin:
            r = csv.DictReader(fin)
            for row in r:
                tid = row.get("id_transazione:ID")
                if not tid:
                    continue
                fout.write(
                    '  <Transazione '
                    f'id="{esc(tid)}" '
                    f'importo="{esc(row.get("importo:INT"))}" '
                    f'data="{esc(row.get("data:DATE"))}">\n'
                )
                fout.write(f'    <MittenteRef matricola="{esc(row.get("matricola"))}"/>\n')
                fout.write(f'    <DestinatarioRef matricola="{esc(row.get("destinatario"))}"/>\n')
                #banca di derivazione della transazione
                fout.write(f'    <BancaDerivaRef id="{esc(row.get("id_banca_deriva"))}"/>\n')
                fout.write("  </Transazione>\n")
        write_close(fout, "Transazioni")

        write_close(fout, "Nodi")
        

        #Footer
        fout.write("</Graph>\n")

    print(f"Creato: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
