#!/usr/bin/env python3
"""
Convert embeddings from parquet to csv

Usage:
python -m src.parse.embeddings \
    --num_threads 10 \
    --input_dir data/wikipedia-2024-06-bge-m3/data/en/ \
    --output_dir data/embeddings/ \
    --insert_aerospike \
    --drop_aerospike
"""
import os
import csv
import argparse
import pandas as pd
import multiprocessing as mp
from glob import glob
from tqdm import tqdm
from src.infra.connections_aerospike import AerospikeConnector


def flatten(x):
    return [item for sublist in x for item in sublist]


def convert_embeddings(input_path, output_dir):
    pbar.set_description(f"Converting {input_path}")
    df = pd.read_parquet(input_path)
    # replace space with _
    df["title"] = df["title"].str.replace(" ", "_")
    # convert embeddings to string delimited by ;
    df["embedding"] = df["embedding"].apply(
        lambda x: ";".join(f"{num:.8f}" for num in x)
    )
    # embedding nodes
    dfn = df.rename(columns={"id": "id:ID", "embedding": "embedding:float[]"}).drop(
        columns=["title"]
    )
    node_path = f"{output_dir}/nodes/embedding/{input_path.split('/')[-1].replace('.parquet', '.csv')}"
    dfn.to_csv(node_path, index=False)
    # embedding to title edges
    dfm = df[["id", "title"]].rename(
        columns={"id": "id:START_ID", "title": "title:END_ID"}
    )
    edge_path = f"{output_dir}/edges/embedding_pages/{input_path.split('/')[-1].replace('.parquet', '.csv')}"
    dfm.to_csv(edge_path, index=False)
    pbar.update(1)
    # insert to aerospike
    if args.insert_aerospike:
        for title in df["title"].tolist():
            aerospike_client.put("wiki", "embedded_pages", title, {"title": title})
    return df["title"].tolist()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="convert embeddings from parquet to csv",
    )
    parser.add_argument(
        "--num_threads",
        type=int,
        default=mp.cpu_count() // 2,
        help="number of threads to use",
    )
    parser.add_argument(
        "--input_dir",
        type=str,
        default="data/wikipedia-2024-06-bge-m3/data/en/",
        help="input directory containing parquet files",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="data/embeddings/",
        help="output directory for csv files",
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
    args = parser.parse_args()

    input_files = glob(f"{args.input_dir}/*.parquet")
    input_files.sort()

    # create directories
    os.makedirs(f"{args.output_dir}/nodes/embedding", exist_ok=True)
    os.makedirs(f"{args.output_dir}/edges/embedding_pages", exist_ok=True)

    # aerospike client
    aerospike_client = AerospikeConnector()
    if args.drop_aerospike:
        aerospike_client.drop_db("wiki", "embedded_pages")

    titles = set()

    pbar = tqdm(total=len(input_files[:20]))
    with mp.Pool(args.num_threads) as pool:
        results = pool.starmap(
            convert_embeddings,
            [
                (
                    input_file,
                    args.output_dir,
                )
                for input_file in input_files[:20]
            ],
        )
    for result in results:
        titles.update(result)

    # save titles
    pd.DataFrame(list(titles), columns=["title:ID"]).to_csv(
        f"{args.output_dir}/nodes/title.csv",
        index=False,
    )
