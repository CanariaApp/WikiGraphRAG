#!/usr/bin/env python3
"""
Parse Wikipedia XML dump and prepare for neo4j admin import

Usage:
python -m src.parse.app \
    --num_threads 10 \
    --filename_input data/wikidump/enwiki-latest-pages-articles-multistream.xml \
    --filename_nodes data/admin/nodes/title.csv \
    --filename_edges data/admin/edges/title_title.csv
"""
import argparse
import csv
import multiprocessing as mp

from src.infra.connections_aerospike import AerospikeConnector
from src.parse.progress_indicator import ProgressIndicator
from src.parse.wikipedia import iterate_pages_from_export_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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
        type=int,
        default=1,
        help="number of seconds between progress updates",
    )
    # insert to aerospike
    parser.add_argument(
        "--insert_to_aerospike",
        action="store_true",
        help="insert data to aerospike",
    )
    args = parser.parse_args()

    # progress indicator
    progress_indicator = ProgressIndicator(
        seconds_between_updates=args.seconds_between_updates
    )

    # Initialize Aerospike Client
    # TO DO: not working
    aerospike_client = AerospikeConnector()

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
                    node_writer=node_writer,
                    edge_writer=edge_writer,
                    aerospike_client=None,
                )
