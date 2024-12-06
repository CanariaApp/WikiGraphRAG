#!/usr/bin/env python3
"""
Parse Wikipedia XML dump and prepare for neo4j admin import

Usage:
nohup python -m src.parse.app \
    --batch_size 100000 \
    --num_threads 4 \
    --filename_input data/wikidump/enwiki-latest-pages-articles-multistream.xml \
    --filename_nodes data/admin/nodes/title.csv \
    --filename_edges data/embeddings/edges/title_title.csv\
    --seconds_between_updates 0.1 \
    > logs/parse.log &
    --insert_edges_csv

"""
import argparse
import csv
import multiprocessing as mp
import bz2
import os
import sys
sys.path.append('.')
import pickle
from src.infra.connections_mongodb import MongoDBJobDB
from src.parse.progress_indicator import ProgressIndicator
from src.parse.wikipedia import iterate_pages_from_export_file,iterate_pages_to_find_redirects

# #Make sure we don't buffer
# class Unbuffered(object):
#     def __init__(self, stream):
#         self.stream = stream
#
#     def write(self, data):
#         self.stream.write(data)
#         self.stream.flush()
#
#     def __getattr__(self, attr):
#         return getattr(self.stream, attr)
# sys.stdout = Unbuffered(sys.stdout)


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
        "--filename_wiki_redirects",
        type=str,
        default="src/data/admin/wiki_redirects.pkl",
        help="name of the pickle file where the wiki page redirects are saved",
    )
    args = parser.parse_args()

    #Get global dictionary for wiki redirects from dump
    if not os.path.exists(args.filename_wiki_redirects):
        # open dump XML and iterate through pages to create a dictionary containing all wiki pages
        print("Creating a dictionary from wiki dump file...")
        # progress indicator
        progress_indicator = ProgressIndicator(
            seconds_between_updates=args.seconds_between_updates
        )
        with bz2.open(args.filename_input, "rt", encoding="utf-8") as xml_file:
           wiki_redirects = iterate_pages_to_find_redirects( xml_file, page_handlers=[progress_indicator.on_element])
        # Save dictionary to pickle file
        with open(args.filename_wiki_redirects, 'wb') as f:
            pickle.dump(wiki_redirects, f)
        print(f"Wiki page dictionary saved to {args.filename_wiki_redirects}")
    else:
        with open(args.filename_wiki_redirects, 'rb') as f:
            wiki_redirects = pickle.load(f)
        print(f"Wiki page dictionary loaded from {args.filename_wiki_redirects}.")

    # ###Fixing uppercase redirects
    # wiki_redirects_new = defaultdict()
    # for key in wiki_redirects:
    #     if ( wiki_redirects[key].lower() !=  key.lower()):
    #         wiki_redirects_new[key.lower()] = wiki_redirects[key].lower()
    # with open(args.filename_wiki_redirects, 'wb') as f:
    #     pickle.dump(wiki_redirects_new, f)


    # Reset progress indicator
    progress_indicator = ProgressIndicator( seconds_between_updates=args.seconds_between_updates)

    # mongodb
    mongodb_client = MongoDBJobDB(
        "mongodb://localhost:27018/",
        "wikidump5x",
    )
    mongodb_client.drop_collection("pages")
    mongodb_client.create_index("pages", "id")

    # open XML file and CSV file simultaneously
    with bz2.open(args.filename_input, "rt", encoding="utf-8") as xml_file:
        with open(args.filename_nodes, "w", newline="", encoding="utf-8") as node_file:
            node_writer = csv.writer(node_file)
            node_writer.writerow(["id:ID", "title", "content"])  # CSV headers
            with open(args.filename_edges, "w", newline="", encoding="utf-8") as edge_file:
                edge_writer = csv.writer(edge_file)
                edge_writer.writerow([":START_ID", ":END_ID"])  # CSV headers

                # Iterate through pages in the XML file
                iterate_pages_from_export_file(
                    xml_file,
                    page_handlers=[progress_indicator.on_element],
                    wiki_redirects=wiki_redirects,
                    node_writer=node_writer if args.insert_nodes_csv else None,
                    edge_writer=edge_writer if args.insert_edges_csv else None,
                    mongodb_client=mongodb_client,
                    batch_size=args.batch_size,
                    num_threads=args.num_threads,
                )
