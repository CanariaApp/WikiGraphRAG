#!/usr/bin/env python3
"""
Parse Wikipedia XML dump and prepare for neo4j admin import

Usage:
python -m src.parse.app \
    --batch_size 100000 \
    --num_threads 4 \
    --filename_input data/wikidump/enwiki-latest-pages-articles-multistream.xml \
    --filename_nodes data/admin/nodes/title.csv \
    --filename_edges data/embeddings/edges/title_title.csv\
    --seconds_between_updates 0.1 \
    --insert_aerospike \
    --drop_aerospike \
    --insert_edges_csv

"""
import argparse
import csv
import multiprocessing as mp
from src.infra.connections_aerospike import AerospikeConnector
from src.infra.connections_mysql import MySQLConnector
from src.parse.progress_indicator import ProgressIndicator
from src.parse.wikipedia import iterate_pages_from_export_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--batch_size",
        type=int,
        default=100000,
        help="batch size for inserting data to database",
    )
    parser.add_argument(
        "--num_threads",
        type=int,
        default=mp.cpu_count() // 2,
        help="number of threads to use",
    )
    parser.add_argument(
        "--filename_input",
        type=str,
        default="data/wikidump/enwiki-latest-pages-articles-multistream.xml",
        help="input filename",
    )
    parser.add_argument(
        "--filename_nodes",
        type=str,
        default="data/admin/nodes/title.csv",
        help="output filename",
    )
    parser.add_argument(
        "--filename_edges",
        type=str,
        default="data/admin/edges/title_title.csv",
        help="output filename",
    )
    parser.add_argument(
        "--seconds_between_updates",
        type=float,
        default=1,
        help="number of seconds between progress updates",
    )
    parser.add_argument(
        "--insert_nodes_csv",
        action="store_true",
        help="insert nodes to csv",
    )
    parser.add_argument(
        "--insert_edges_csv",
        action="store_true",
        help="insert edges to csv",
    )
    parser.add_argument(
        "--insert_aerospike",
        action="store_true",
        help="insert data to aerospike",
    )
    parser.add_argument(
        "--drop_aerospike",
        action="store_true",
        help="drop and recreate aerospike namespace",
    )
    parser.add_argument(
        "--insert_mysql",
        action="store_true",
        help="insert data to mysql",
    )
    parser.add_argument(
        "--drop_mysql",
        action="store_true",
        help="drop and recreate mysql tables",
    )
    args = parser.parse_args()

    # progress indicator
    progress_indicator = ProgressIndicator(
        seconds_between_updates=args.seconds_between_updates
    )

    # aerospike client
    aerospike_client = AerospikeConnector()
    if args.drop_aerospike:
        aerospike_client.drop_db("wiki", "page_links")

    # mysql client
    mysql_client = MySQLConnector(
        "wikiuser",
        "wikidump5x",
        "userWiki",
        port=3307,
    )
    # Define the table schema
    table_schema = {
        # "title_link_hash": "CHAR(32)",
        # "title_hash": "CHAR(32)",
        "title": "VARCHAR(2047)",
        # "link_hash": "CHAR(32)",
        "link": "VARCHAR(2047)",
        "pos": "INT"
    }
    if args.drop_mysql:
        if "wiki_links" in mysql_client.get_all_tables():
            mysql_client.delete_table("wiki_links")
        mysql_client.create_table(
            table_name="wiki_links", 
            columns=table_schema, 
            verbose=True,
        )
        # # too slow to insert after creating an index
        # mysql_client.create_index(
        #     table_name="wiki_links", 
        #     columns=["title_link_hash", "title_hash", "link_hash"], 
        #     unique=False,  # Optional: Set to True if you want a unique index
        #     verbose=True
        # )

    # open XML file and CSV file simultaneously
    with open(args.filename_input, "r", encoding="utf-8") as xml_file: 
        with open(args.filename_nodes, "w", newline="", encoding="utf-8") as node_file:
            node_writer = csv.writer(node_file)
            node_writer.writerow(["title:ID"])  # CSV headers
            with open(args.filename_edges, "w", newline="", encoding="utf-8") as edge_file:
                edge_writer = csv.writer(edge_file)
                edge_writer.writerow(["title:START_ID", "title:END_ID", "pos"])  # CSV headers

                # Iterate through pages in the XML file
                iterate_pages_from_export_file(
                    xml_file,
                    page_handlers=[progress_indicator.on_element],
                    node_writer=node_writer if args.insert_nodes_csv else None,
                    edge_writer=edge_writer if args.insert_edges_csv else None,
                    mysql_client=mysql_client if args.insert_mysql else None,
                    aerospike_client=None if not args.insert_aerospike else aerospike_client,
                    batch_size=args.batch_size,
                    num_threads=args.num_threads,
                )
