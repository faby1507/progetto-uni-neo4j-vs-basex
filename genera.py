import csv
import random
from faker import Faker
from datetime import date, timedelta
from collections import defaultdict

fake = Faker('it_IT') #imposta la lingua italiana
random.seed(42)

#target complessivo 500.000
NUM_PERSONE = 100_000
NUM_TRANS   = 100_000
NUM_BANCHE  = 100_000
NUM_FONTI   = 100_000

#pattern sospetti: una quota delle persone riceve più transazioni in 1 giorno > max_deposito
NUM_PATTERN_SOSPETTI = min(10_000, int(NUM_PERSONE * 0.1))  #10% in proporzione al numero persone

#crea banche e fonti uniche 
banche_ids = [f"b{i+1}" for i in range(NUM_BANCHE)] #crea stringa che inizia con b e aggiunge numeri
fonti_ids  = [f"f{i+1}" for i in range(NUM_FONTI)] #idem con f

#genera banche come dizionario 
banche = {
    bid: { #chiave esterna (l'ID della banca)
        "id_banca": bid, #campo interno
        "nome": fake.company() + " Bank",
        "nazione": fake.country_code(),
        "max_deposito": random.randint(1500, 10000)
    }
    for bid in banche_ids
}
#genera fonti come dizionario 
fonti = {
    fid: {
        "id_fonte": fid,
        "nome": fake.company(),
        "nazione": fake.country_code(),
        "affidabilita": round(random.uniform(0.5, 1.0), 2)
    }
    for fid in fonti_ids
}

#persone e documenti (1:1)
persone = []      #[matricola, nome, cognome, stipendio, id_banca, id_documento, id_fonte]
documenti = []    #[id_documento, nazione, email, scadenza, matricola, num_telefono]

#per generare un pool di email/num telefono che poi avrà duplicati 
emails_pool   = [fake.email() for _ in range(2000)]
phones_pool   = [fake.phone_number() for _ in range(2000)]

for i in range(1, NUM_PERSONE + 1):
    matricola = f"p{i}"
    id_doc = f"d{i}" 
    id_banca = random.choice(banche_ids) #sceglie una banca a caso 
    id_fonte = random.choice(fonti_ids) #sceglie fonte a caso 

    persone.append([
        matricola,
        fake.first_name(),
        fake.last_name(),
        random.randint(1000, 5000),
        id_banca,
        id_doc,
        id_fonte
    ])

    email = random.choice(emails_pool) if random.random() < 0.1 else fake.email()
    phone = random.choice(phones_pool) if random.random() < 0.1 else fake.phone_number()
    #per ogni persona/documento:
    #con probabilità 10% prende un valore a caso dalla pool già esistente per fare un duplicato potenziale.
    #con probabilità 90% genera una nuova email/telefono nuovo con fake.email() o fake.phone_number().

    documenti.append([ #genera documenti 
        id_doc,
        fake.country_code(),
        email,
        (date.today() + timedelta(days=random.randint(365, 5*365))).isoformat(), #scadenza
        matricola,
        phone
    ])

#salva chi è legato a chi per agevolare le generazione di transazioni e evitare incomgruenze 
#es. la banca del mittente non viene mai scelta a caso, ma letta da person_to_bank
person_to_bank = {p[0]: p[4] for p in persone}                          # mittente -> id_banca
banca_limits   = {bid: b["max_deposito"] for bid, b in banche.items()}  # id_banca -> massimale
doc_by_matricola = {d[4]: d for d in documenti}                         # matricola -> documento

#transazioni
#transazioni: [id_trans, mittente_matricola, importo, destinatario_matricola, data_str, id_banca_deriva]
transazioni = []

def add_tx(mid, dest, imp, day):
    id_banca_deriva = person_to_bank[mid] #recupera dal dizionario person_to_bank la banca associata al mittente
    transazioni.append(
        [f"t{len(transazioni)+1}", mid, imp, dest, day, id_banca_deriva])

oggi = date.today()

#pattern sospetti su destinatari (somma giornaliera > max_deposito della banca del destinatario)
NUM_PATTERN_SOSPETTI = min(NUM_PATTERN_SOSPETTI, NUM_PERSONE) #assicura che non chieda mai più sospetti del numero totale di persone
persone_dest_sospette = random.sample(persone, NUM_PATTERN_SOSPETTI) #random.sample(lista, k) estrae k elementi unici a caso da lista.
#prende NUM_PATTERN_SOSPETTI persone diverse dalla lista persone.
#il risultato è una lista di persone che saranno usate come destinatari sospetti (cioè riceveranno troppe transazioni rispetto al limite della loro banca).

#mappa email/telefono -> liste di persone (per rendere mittenti plausibili)
email_to_people = defaultdict(list)#con un defaultdict(list), se accedo a una chiave che non esiste Python crea automaticamente una nuova lista vuota [] per quella chiave.
phone_to_people = defaultdict(list)#grazie al defaultdict(list), non deve controllare se la chiave esiste: può sempre fare .append(...) in sicurezza.
for id_doc, _, email, _, matricola, phone in documenti:
    email_to_people[email].append(matricola) #aggiunge la matricola alla lista di persone che usano quella email.
    phone_to_people[phone].append(matricola) #idem. essendo defaultdict alla prima occorrenza di una chiave viene creata automaticamente una lista vuota, quindi .append(...) non genera errori

for destinatario in persone_dest_sospette:
    if len(transazioni) >= NUM_TRANS: #Se ha raggiunto o superato il target, break esce dal ciclo corrente e smette di aggiungere altre transazioni
        break
    dest_matr = destinatario[0]
    dest_banca = destinatario[4]
    max_dep_dest = banca_limits[dest_banca] #prende la matricola del destinatario, la sua banca, con quella banca cerca nel dizionario banca_limits il max_deposito da usare come soglia

    day = (oggi - timedelta(days=random.randint(1, 60))).isoformat()

    doc_dest = doc_by_matricola[dest_matr] #Recupera il documento della persona destinataria, usando la sua matricola come chiave nel dizionario di lookup.
    email = doc_dest[2]
    phone = doc_dest[5]

    mittenti_pot = set(email_to_people[email]) | set(phone_to_people[phone]) #Prende tutte le matricole che condividono la stessa email oppure lo stesso telefono del destinatario e fa l’unione (|). L’uso dei set elimina eventuali duplicati in automatico.
    mittenti_pot.discard(dest_matr) #Rimuove il destinatario dall’insieme (niente auto-transazioni)
    if not mittenti_pot: #non dà errore se l’elemento non c’è (a differenza di remove)
        continue

    n_mitt = min(random.randint(2, 4), len(mittenti_pot)) #estrae un numero intero a caso tra 2 e 4. non supera mai il numero di candidati disponibili.
    mittenti = random.sample(list(mittenti_pot), n_mitt) #sceglie n_mitt elementi distinti dall’insieme dei candidati

    base = max(300, max_dep_dest // n_mitt + random.randint(200, 700))#basa le trasazioni sul massimale della banca del destinatario
    for m in mittenti:
        if len(transazioni) >= NUM_TRANS:
            break
        add_tx(m, dest_matr, base, day)

#transazioni normali distribuite. almeno una per persona
for p in persone:
    if len(transazioni) >= NUM_TRANS:
        break
    mittente = p[0]
    dest = f"p{random.randint(1, NUM_PERSONE)}"
    id_banca_mitt = p[4]
    max_dep_mitt = banca_limits[id_banca_mitt]
    imp = random.randint(10, min(3000, max_dep_mitt // 2))
    day = (oggi - timedelta(days=random.randint(1, 730))).isoformat()
    add_tx(mittente, dest, imp, day)

#riempie se mancano transazioni
while len(transazioni) < NUM_TRANS:
    mitt = f"p{random.randint(1, NUM_PERSONE)}"
    dest = f"p{random.randint(1, NUM_PERSONE)}"
    id_b = person_to_bank[mitt]
    imp = random.randint(10, min(3000, banca_limits[id_b] // 2))
    day = (oggi - timedelta(days=random.randint(1, 730))).isoformat()
    add_tx(mitt, dest, imp, day)

#scrittura CSV (UTF-8)
with open("persone.csv", "w", newline='', encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(['matricola:ID', 'nome', 'cognome', 'stipendio:INT', 'id_banca', 'id_documento', 'id_fonte', ':LABEL'])
    for r in persone:
        w.writerow(r + ['Persona'])

with open("documenti.csv", "w", newline='', encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(['id_documento:ID', 'nazione', 'email', 'scadenza', 'matricola', 'num_telefono', ':LABEL'])
    for r in documenti:
        w.writerow(r + ['Documento'])

with open("banche.csv", "w", newline='', encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(['id_banca:ID', 'nome', 'nazione', 'max_deposito:INT', ':LABEL'])
    for b in banche.values():
        w.writerow([b["id_banca"], b["nome"], b["nazione"], b["max_deposito"], 'Banca'])

with open("fonti.csv", "w", newline='', encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(['id_fonte:ID', 'nome', 'nazione', 'affidabilita:FLOAT', ':LABEL'])
    for ft in fonti.values():
        w.writerow([ft["id_fonte"], ft["nome"], ft["nazione"], ft["affidabilita"], 'Fonte'])


with open("transazioni.csv", "w", newline='', encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(['id_transazione:ID', 'matricola', 'importo:INT', 'destinatario', 'data:DATE', 'id_banca_deriva', ':LABEL'])
    for r in transazioni:
        w.writerow(r + ['Transazione'])

#CSV relazioni DERIVA per import relazionale
#colonne in stile neo4j-admin
with open("deriva.csv", "w", newline='', encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(['id_banca:START_ID', 'id_transazione:END_ID', ':TYPE'])
    for tid, mid, imp, dest, day, bid in transazioni:
        w.writerow([bid, tid, 'DERIVA'])
#Per ogni transazione (tid, …, bid), scrive una riga: bid -> tid con tipo DERIVA.
#Esempio: b7,t123,DERIVA = relazione (:Banca {id:'b7'})-[:DERIVA]->(:Transazione {id:'t123'}).
print("CSV generati.")
print("Conteggi righe:",
      "persone", len(persone),
      "documenti", len(documenti),
      "banche", len(banche),
      "fonti", len(fonti),
      "transazioni", len(transazioni),
      "totale", len(persone) + len(documenti) + len(banche) + len(fonti) + len(transazioni))
