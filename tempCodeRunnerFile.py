MATCH (p:Persona)
WITH p
ORDER BY p.ID_Locale

// 2) Raggruppo in un’unica passata, raccogliendo in una lista già ordinata
WITH p.Nome   AS Nome,
     p.Cognome AS Cognome,
     collect(p) AS persons

// 3) Determino per ogni gruppo il primo ID_Locale incontrato
WITH Nome,
     Cognome,
     persons,
     persons[0].ID_Locale AS firstID

// 4) Ordino i gruppi in base a quel firstID (così i tuoi <Identity> escono nello stesso ordine)
ORDER BY firstID

// 5) Costruisco l’output esattamente come in XQuery
RETURN
  Nome,
  Cognome,
  size(persons) AS Count,
  [ m IN persons | {
      ID_Locale:    m.ID_Locale,
      ID_Documento: m.ID_Documento,
      ID_Fonte:     m.ID_Fonte
  } ] AS Matches;