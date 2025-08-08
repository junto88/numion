from __future__ import annotations
import os
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI
from pydantic import BaseModel
from neo4j import GraphDatabase
import spacy
from dotenv import load_dotenv
load_dotenv()

URI = os.getenv("NUMION_NEO4J_URI")
USER = os.getenv("NUMION_NEO4J_USER")
PASSWORD = os.getenv("NUMION_NEO4J_PASSWORD")

if not URI or not PASSWORD:
    raise RuntimeError(
        "Config mancante: imposta NUMION_NEO4J_URI e NUMION_NEO4J_PASSWORD nelle variabili d'ambiente."
    )

# Connessione Neo4j (Aura)
driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# Modello NLP (spaCy)
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    # Messaggio amichevole se il modello non è stato scaricato
    raise RuntimeError(
        "Modello spaCy mancante. Esegui: python -m spacy download en_core_web_sm"
    )

app = FastAPI(title="NUMION API", version="0.1.0")


# ========= Schemi =========
class IngestBody(BaseModel):
    text: str
    source: Optional[str] = None
    doc_id: Optional[str] = None


# ========= Utils Neo4j =========
def cypher_ingest(tx, doc_id: str, source: str, created_at: str, entities: List[dict]):
    """
    Crea/merge un Document, le Entity menzionate e le relazioni MENTIONS/CO_OCCURS_WITH.
    entities: lista di dict con {name, label} (label = tipo entità spaCy es. ORG, GPE, MONEY)
    """
    # Documento
    tx.run(
        """
        MERGE (d:Document {id:$doc_id})
        ON CREATE SET d.source=$source, d.created_at=$created_at
        """,
        doc_id=doc_id,
        source=source,
        created_at=created_at,
    )

    # Entità + MENTIONS
    for ent in entities:
        tx.run(
            """
            MERGE (e:Entity {name:$name})
            ON CREATE SET e.type=$etype
            ON MATCH  SET e.type=coalesce(e.type, $etype)
            MERGE (d:Document {id:$doc_id})-[:MENTIONS]->(e)
            """,
            name=ent["name"],
            etype=ent["label"],
            doc_id=doc_id,
        )

    # Relazioni di co-occorrenza (peso cumulativo)
    for i in range(len(entities)):
        for j in range(i + 1, len(entities)):
            a, b = entities[i]["name"], entities[j]["name"]
            tx.run(
                """
                MATCH (ea:Entity {name:$a}), (eb:Entity {name:$b})
                MERGE (ea)-[r:CO_OCCURS_WITH]->(eb)
                ON CREATE SET r.weight = 1
                ON MATCH  SET r.weight = coalesce(r.weight,0) + 1
                """,
                a=a,
                b=b,
            )


# ========= Endpoints =========
@app.get("/")
def read_root():
    return {"message": "Welcome to NUMION API!"}


@app.get("/health/neo4j")
def health_neo4j():
    try:
        with driver.session() as s:
            res = s.run("RETURN 'OK' AS status").single()
            return {"neo4j": res["status"]}
    except Exception as e:
        return {"neo4j": "ERROR", "detail": str(e)}


@app.post("/process_text/")
def process_text(text: str):
    """
    Estrae entità dal testo e le ritorna (non salva).
    """
    doc = nlp(text)
    entities = [{"name": ent.text, "label": ent.label_} for ent in doc.ents]
    return {"entities": entities}


@app.post("/ingest_text/")
def ingest_text(body: IngestBody):
    """
    Estrae entità dal testo, crea un nodo Document, nodi Entity e relazioni in Neo4j.
    """
    doc_id = body.doc_id or f"doc_{int(datetime.utcnow().timestamp())}"
    source = body.source or "manual"

    doc = nlp(body.text)
    entities = [{"name": ent.text, "label": ent.label_} for ent in doc.ents]

    try:
        with driver.session() as session:
            session.write_transaction(
                cypher_ingest, doc_id, source, datetime.utcnow().isoformat(), entities
            )
    except Exception as e:
        return {"error": str(e)}

    return {"doc_id": doc_id, "entities": entities, "count": len(entities)}


@app.get("/graph/entities")
def graph_entities(limit: int = 25):
    """
    Restituisce un elenco di nomi di entità presenti nel grafo.
    """
    with driver.session() as s:
        recs = s.run(
            """
            MATCH (e:Entity)
            RETURN e.name AS name, e.type AS type
            LIMIT $limit
            """,
            limit=limit,
        )
        return {"entities": [{"name": r["name"], "type": r["type"]} for r in recs]}


@app.get("/graph/neighbors")
def graph_neighbors(entity: str, limit: int = 25):
    """
    Restituisce i vicini (co-occorrenze) di un'entità.
    """
    with driver.session() as s:
        recs = s.run(
            """
            MATCH (e:Entity {name:$entity})- [r:CO_OCCURS_WITH] -> (n)
            RETURN n.name AS neighbor, r.weight AS weight
            ORDER BY weight DESC
            LIMIT $limit
            """,
            entity=entity,
            limit=limit,
        )
        return {"entity": entity, "neighbors": [{"name": r["neighbor"], "weight": r["weight"]} for r in recs]}

