#!/usr/bin/env python
"""
Insert exported data to Neo4j database
Don't use APOC for batch insertion

Usage:
nohup \
python -m hf_embeddings \
    --batch_size 10000 \
    --neo4j_uri bolt://localhost:7687 \
    --neo4j_user neo4j \
    --neo4j_password password \
    > logs/hf_embeddings.log &
"""
import pandas as pd
import argparse
from datasets import load_dataset
from tqdm import tqdm
from neo4j import GraphDatabase

def insert_into_neo4j(driver, data):
    with driver.session() as session:
        for record in tqdm(data):
            url = record["url"]
            title = record["title"]
            text = record["text"]
            embedding = record["embedding"]

            # Create a Paragraph node with the specified attributes and connect it to the existing Page node based on title
            session.run(
                """
                MATCH (page:Page {title: $title})
                CREATE (para:Paragraph {id: $id, title: $title, url: $url, text: $text, embedding: $embedding})
                CREATE (page)-[:HAS_PARAGRAPH]->(para)
                """,
                id=record["id"],
                title=title,
                url=url,
                text=text,
                embedding=embedding
            )

def bulk_insert_into_neo4j(driver, data, batch_size=1000):
    with driver.session() as session:
        for i in tqdm(range(0, len(data), batch_size)):
            batch = data[i:i + batch_size]

            session.run(
                """
                UNWIND $batch AS row
                MATCH (page:Page {title: row.title})
                CREATE (para:Paragraph {id: row.id, title: row.title, url: row.url, text: row.text, embedding: row.embedding})
                CREATE (page)-[:HAS_PARAGRAPH]->(para)
                """,
                batch=batch
            )

def insert_into_neo4j_apoc(driver, data):
    # convert data to a format suitable for APOC
    batch_data = [{"id": record["id"], "title": record["title"], "url": record["url"], "text": record["text"], "embedding": list(record["embedding"])} for record in data]

    with driver.session() as session:
        # APOC batch command
        session.run(
            """
            CALL apoc.periodic.iterate(
              'UNWIND $batch AS row RETURN row',
              'MATCH (page:Page {title: row.title})
               CREATE (para:Paragraph {id: row.id, title: row.title, url: row.url, text: row.text, embedding: row.embedding})
               CREATE (page)-[:HAS_PARAGRAPH]->(para)',
              {batchSize: 1000, parallel: true, params: {batch: $batch}}
            )
            """,
            batch=batch_data
        )


if __name__ == "__main__":
    # Argument parser for batch size
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--batch_size",
        type=int,
        default=10000,
        help="Batch size for inserting data into Neo4j",
    )
    parser.add_argument(
        "--neo4j_uri",
        type=str,
        default="bolt://localhost:7687",
        help="URI of the Neo4j instance"
    )
    parser.add_argument(
        "--neo4j_user",
        type=str,
        default="neo4j",
        help="Neo4j username"
    )
    parser.add_argument(
        "--neo4j_password",
        type=str,
        default="password",
        help="Neo4j password"
    )
    parser.add_argument(
        "--use_apoc",
        action="store_true",
        help="Use APOC for batch insertion",
    )
    args = parser.parse_args()
    
    # load dataset
    dataset = load_dataset(
        "Upstash/wikipedia-2024-06-bge-m3",
        "en",
        split="train",
        streaming=True,
    )

    driver = GraphDatabase.driver(
        args.neo4j_uri, 
        auth=(args.neo4j_user, args.neo4j_password),
    )

    bulk = []
    for data in tqdm(dataset):
        data_id = data["id"]
        url = data["url"]
        title = data["title"]
        text = data["text"]
        embedding = data["embedding"]

        bulk.append(data)

        if len(bulk) == args.batch_size:
            if args.use_apoc:
                insert_into_neo4j_apoc(driver, bulk)
            else:
                bulk_insert_into_neo4j(driver, bulk, args.batch_size//10)
            bulk = []

    driver.close()
