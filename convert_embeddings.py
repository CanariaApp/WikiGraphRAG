#!/usr/bin/env python3
"""
Convert embeddings from parquet to csv

Usage:
python -m convert_embeddings \
    --num_threads 10 \
    --input_dir wikipedia-2024-06-bge-m3/data/en/ \
    --output_dir data/embeddings/
"""
import argparse
import pandas as pd
import multiprocessing as mp
from glob import glob
from tqdm import tqdm


def convert_embeddings(input_path, output_path):
    pbar.set_description(f"Converting {input_path}")
    embeddings = pd.read_parquet(input_path)
    # convert embeddings to string delimited by ;
    embeddings["embedding"] = embeddings["embedding"].apply(lambda x: ";".join(map(str, x)))
    embeddings.to_csv(output_path, index=False)
    pbar.update()
    return

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
        default="wikipedia-2024-06-bge-m3/data/en/",
        help="input directory containing parquet files",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="data/embeddings/",
        help="output directory for csv files",
    )
    args = parser.parse_args()

    input_files = glob(f"{args.input_dir}/*.parquet")
    input_files.sort()

    pbar = tqdm()
    with mp.Pool(args.num_threads) as pool:
        pool.starmap(
            convert_embeddings,
            [
                (
                    input_file,
                    input_file.replace(".parquet", ".csv").replace(
                        args.input_dir, args.output_dir
                    ),
                )
                for input_file in input_files[:10]
            ],
        )
