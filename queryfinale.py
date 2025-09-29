
import time
import math
import statistics
from neo4j import GraphDatabase
import BaseXClient
import pandas as pd
import os

#configurazione BaseX 
HOST = "localhost"
PORT = 1984
USERNAME = "admin"
PASSWORD = "1234"
DATABASE = "dataset_100"

#configurazione Neo4j 
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

#output risultati 
RESULTS_DIR = r"C:\Users\fabyp\OneDrive\Documents\Progetto DB2\risultati"
RESULTS_FILE = os.path.join(RESULTS_DIR, "benchmark_risultati_100.xlsx")

#query  

QUERIES = [
  {
        "name": "Query 1",
        "cypher": """
match (b:Banca{nazione:'GE'}) return b.nome;

""",
        "xquery": r'''
xquery version "3.1";

for $b in /Graph/Nodi/Banche/Banca[@nazione = 'GE']
return <nome>{ $b/Nome/text() }</nome>


'''
    },
  {
        "name": "Query 2",
        "cypher": """
match (f:Fonte) where f.affidabilita<=0.6 and f.nome starts with 'P'
    return f.nome

""",
        "xquery": r'''
xquery version "3.1";

for $f in /Graph/Nodi/Fonti/Fonte
let $a := xs:decimal($f/@affidabilita)
let $n := normalize-space($f/Nome)
where $a le 0.6 and starts-with($n, 'P')
return <nome>{ $n }</nome>


'''
    },

       {
        "name": "Query 3",
        "cypher": """
match (d:Documento)
WITH d.email AS c1, count(*) AS n
RETURN c1, n
order by n DESC;

""",
        "xquery": r'''
xquery version "3.1";


for $d in /Graph/Nodi/Documenti/Documento
let $c1 := $d/Email/text()  
where string-length($c1) > 0
group by $c1
let $n := count($d)
order by $n descending, $c1 ascending 
return <record c1="{$c1}" n="{$n}"/>


'''
    },
 
{
        "name": "Query 4",
        "cypher": """
      MATCH (p:Persona)-[:HA_BANCA]->(b:Banca)
MATCH (t:Transazione)
WHERE t.destinatario = p.matricola
WITH p.matricola AS matricola, date(t.data) AS giorno, sum(t.importo) AS totale, b.max_deposito AS max
WHERE totale > max
RETURN matricola, giorno, totale, max
ORDER BY totale DESC;



""",
        "xquery": r'''

xquery version "3.1";

for $g in /Graph/Nodi/Transazioni/Transazione
           group by $dest := $g/DestinatarioRef/@matricola/string(),
                    $day  := $g/@data/string()
let $tot := sum($g/@importo ! xs:integer(.))
let $bid := /Graph/Nodi/Persone/Persona[@matricola = $dest]/BancaRef/@id/string()
let $max := xs:integer(/Graph/Nodi/Banche/Banca[@id = $bid]/@max_deposito)
where $dest and $day and $max and $tot > $max
order by $tot descending, $dest ascending, $day ascending
return
  <sospetto matricola="{$dest}"
            giorno="{$day}"
            totale="{$tot}"
            max="{$max}"/>

'''
    }
]


#statistiche
def confidence_interval_95(times):
    n = len(times)        #conta elementi nella lsita 
    if n < 2:
        return 0.0  #evita eccezioni come /0
    mean = statistics.mean(times) #media
    stdev = statistics.stdev(times) #deviazioen standard
    t = 2.045 if n == 30 else 1.96 
    ci = t * (stdev / math.sqrt(n))
    return ci

#misurazioni
def measure_basex(xquery):
    times = []
    try:
        session = BaseXClient.Session(HOST, PORT, USERNAME, PASSWORD) #connessione
        session.execute(f"open {DATABASE}")
        
        
        for i in range(31):  #1 esec + 30 misure
            start = time.perf_counter() #restituisce un timestamp 
            session.execute(f"xquery {xquery}") #fa la query 
            end = time.perf_counter() #timestamp fine 
            times.append((end - start) * 1000.0) #converte il tempo trascorso in ms 
    except Exception as e:
        try:
            session.close() #chiude sessione in caso di errori 
        except:
            pass #se la sessione è già chiusa ignora eccezione
        return float("nan"), float("nan"), float("nan"), f"Errore BaseX: {e}" #gestisce errori basex 
    finally:
        try:
            session.close() #chiude sessione
        except Exception:
            pass
    
    first = times[0] #salva la prima esec
    rest = times[1:] if len(times) > 1 else [times[0]] #salva le restanti 
    avg = sum(rest) / len(rest) #media
    ci = confidence_interval_95(rest)
    return first, avg, ci, None #none se non ci sono errori 

def measure_neo4j(cypher):
    times = []
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            #forza connessione fuori dal cronometro
            driver.verify_connectivity()
            session.run("RETURN 1").consume()

            #1 warm-up + 30 misure reali
            for i in range(31):
                start = time.perf_counter()
                session.run(cypher).data()  #ogni riga della query diventa un dizionario
                end = time.perf_counter()
                times.append((end - start) * 1000.0)
    except Exception as e:
        return float("nan"), float("nan"), float("nan"), f"Errore Neo4j: {e}"
    finally:
        try:
            driver.close()
        except Exception:
            pass

    first = times[0]
    rest = times[1:] if len(times) > 1 else [times[0]]
    avg = sum(rest) / len(rest)
    ci = confidence_interval_95(rest)
    return first, avg, ci, None



#Main
if __name__ == "__main__":
    os.makedirs(RESULTS_DIR, exist_ok=True)
    results = []

    print(f"{'Query':<12} | {'DBMS':<6} | {'Prima (ms)':>10} | {'Media 30 (ms)':>12} | {'CI 95% (ms)':>12}")
    print("-" * 85)

    #PRIMA Neo4j per tutte le query
    for q in QUERIES:
        first_n, avg_n, ci_n, err_n = measure_neo4j(q.get("cypher", "").strip())
        print(f"{q['name']:<12} | {'Neo4j':<6} | {first_n:10.2f} | {avg_n:12.2f} | {ci_n:12.2f}")
        results.append({
            "Query": q["name"], "DBMS": "Neo4j",
            "Prima(ms)": round(first_n, 2),
            "Media30(ms)": round(avg_n, 2),
            "CI95(ms)": round(ci_n, 2)
        })

    #POI BaseX per tutte le query
    for q in QUERIES:
        first_b, avg_b, ci_b, err_b = measure_basex(q.get("xquery", "").strip())
        print(f"{q['name']:<12} | {'BaseX':<6} | {first_b:10.2f} | {avg_b:12.2f} | {ci_b:12.2f}")
        results.append({
            "Query": q["name"], "DBMS": "BaseX",
            "Prima(ms)": round(first_b, 2),
            "Media30(ms)": round(avg_b, 2),
            "CI95(ms)": round(ci_b, 2)
        })

    #salvataggio
    df = pd.DataFrame(results, columns=["Query", "DBMS", "Prima(ms)", "Media30(ms)", "CI95(ms)", "Note"])
    df.to_excel(RESULTS_FILE, index=False)
    print(f"\nRisultati salvati in {RESULTS_FILE}")

#Trovare banche in germania 
#Trovare fonti con poca affidabilità che iniziano con p 
#Trovare conta email ripetute nei documenti 
#Trovare persone che hanno ricevuto in entrata più di quanto permette la loro banca in un giorno solo
