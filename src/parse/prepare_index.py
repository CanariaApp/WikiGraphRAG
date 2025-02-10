#!/usr/bin/env python3
"""
Parse Wikipedia XML dump and prepare for neo4j admin import

Usage:
 nohup python ./src/parse/prepare_index.py
    --embeddings_dir=src/data/embeddings
    --filename_wiki_redirects=src/data/admin/wiki_redirects.pkl
    > log.txt &

"""

import argparse
import os
import numpy as np
import pickle
import faiss
from collections import defaultdict
from wikipedia import ParquetIterator
from datasets import load_dataset

def par_list_to_ids_and_vectors(par_list, wiki_redirects=defaultdict(None)):
    par_ids = [wiki_redirects.get(p["title"].lower(), p["title"].lower())+'_'+p["id"].split("_")[1] for p in par_list]
    par_vectors = np.stack([p["embedding"] for p in par_list])
    return par_ids,par_vectors

def init_bge3_data(embeddings_dir):
    #Get vector data
    if embeddings_dir is not None:
        ##### BGE3 from downloaded files
        # Start reading in bge3 dataset for page paragraphs
        print("Loading embeddings from "+embeddings_dir)
        bge3_dataset = ParquetIterator(embeddings_dir)
    else:
        ##### BGE3 streamed from Huggingface
        # #Start streaming in the bge3 dataset for page paragraphs
        print("Streaming embeddings from Huggingface")
        bge3_dataset = iter(load_dataset("Upstash/wikipedia-2024-06-bge-m3", "en", split="train", streaming=True))
    return bge3_dataset

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--embeddings_dir",
        type=str,
        default=None,
        help="location of the embeddings parquet files, if None, then the embeddings are streamed from Huggingface",
    )
    parser.add_argument(
        "--filename_wiki_redirects",
        type=str,
        default="src/data/admin/wiki_redirects.pkl",
        help="name of the pickle file where the wiki page redirects are saved",
    )
    parser.add_argument(
        "--reservoir_size",
        type=int,
        default=200000,
        help="size of the representative sample used to train the index",
    )
    parser.add_argument(
        "--index_filename",
        type=str,
        default="src/data/admin/bge3_index.pkl",
        help="name of the pickle file where the wiki page redirects are saved",
    )
    parser.add_argument(
        "--index_ncentroids",
        type=int,
        default=5000,
        help="Number of centroids for index",
    )
    parser.add_argument(
        "--index_nsubquantizers",
        type=int,
        default=128,
        help="number of subquantizers for index",
    )
    parser.add_argument(
        "--index_nbits",
        type=int,
        default=8,
        help="specifies that each sub-vector is encoded as nbits bits",
    )
    args = parser.parse_args()

    #Get BGE3 embedding data
    bge3_dataset = init_bge3_data(args.embeddings_dir)

    with open(args.filename_wiki_redirects, 'rb') as f:
        wiki_redirects = pickle.load(f)
    print(f"Wiki page dictionary loaded from {args.filename_wiki_redirects}.")



    #Init index
    quantizer = faiss.IndexFlatL2(1024) #BGE3 has a dimensionality of 1024
    index = faiss.IndexIVFPQ(quantizer, 1024, args.index_ncentroids, args.index_nsubquantizers, args.index_nbits)

    #Build a representative sample with reservoir sampling
    print("Building representative sample...")
    reservoir=[]
    reservoir_ind = [] #to keep track of what indices we put into the reservoir
    for i,p in enumerate(bge3_dataset):
        if not (i%100000): print("   Processing element %d",i)
        if (i<args.reservoir_size):
            reservoir.append(p)
            reservoir_ind.append(i)
        else:
            k = np.random.randint(0, high=i+1)
            if (k < args.reservoir_size):
                reservoir[k] = p
                reservoir_ind[k] = i
    par_ids, par_vectors = par_list_to_ids_and_vectors(reservoir, wiki_redirects=wiki_redirects)
    #discard reservoir
    reservoir = []
    #train index on representative sample
    index.train(par_vectors)
    index.add(par_vectors)
    #Init index to id match list
    full_id_list=par_ids
    print("Index trained on representative sample of size %d", args.reservoir_size)

    print("Adding remaining vectors to index...")
    #Reset data stream
    bge3_dataset = init_bge3_data(args.embeddings_dir)
    #Get a sorted list of indices that are already in the reservoir that we can iterate over
    reservoir_ind_iter = iter(np.sort(reservoir_ind))
    next_res_ind = next(reservoir_ind_iter)
    #Add the rest to the index, so we need to go over again and add it to the index in reservoir_size chunks
    par_list = []
    for i,p in enumerate(bge3_dataset):
        if not (i % 100000): print("   Processing element %d")
        if (i==next_res_ind): #this means this one was already in the reservoir so we don't want to put it in twice
            next_res_ind = next(reservoir_ind_iter, -1)
        else:
            par_list.append(p)
            #If we collected enough, we should add them to the index and discard the data to save memory
            if (len(par_list)==args.reservoir_size):
                par_ids, par_vectors = par_list_to_ids_and_vectors(par_list, wiki_redirects=wiki_redirects)
                #reset list
                par_list = []
                #Add to index
                index.add(par_vectors)
                #keep ids
                full_id_list += par_ids
    #Do the final ones
    par_ids, par_vectors = par_list_to_ids_and_vectors(par_list, wiki_redirects=wiki_redirects)
    #Add to index
    index.add(par_vectors)
    #keep ids
    full_id_list += par_ids
    print("All %d vectors added to index...",i)

    #Save index data
    with open(args.index_filename, 'wb') as f:
        pickle.dump([index, full_id_list], f)
    print("Index saved to "+args.index_filename)
#     kkk
#
#
#
#
#
#
#
# path="../data/embeddings/000.parquet"
# os.path.exists(path)
# data = pd.read_parquet(path)
# items = data["id"]
# vectors1 = np.stack(data["embedding"])
# #vectors=vectors1+0
# d=vectors1.shape[1]
#
# data = pd.read_parquet("../data/embeddings/001.parquet")
# vectors2 = np.stack(data["embedding"])
# #index.add(vectors2)
# #print(asizeof.asizeof(index))
# vectors = np.vstack([vectors1,vectors2])
#
#
# ncentroids = 5000
# m = 128                             # number of subquantizers, def 8
# nbits = 8 #specifies that each sub-vector is encoded as nbits bitss
# k = 4 #number of nearest neighbors
# quantizer = faiss.IndexFlatL2(d)  # this remains the same
# index = faiss.IndexIVFPQ(quantizer, d, ncentroids, m, nbits)
# index.train(vectors)
#
# #index = faiss.IndexFlatL2(d)  #
#
#
# index.add(vectors)
# D, I = index.search(vectors[:5], k) # sanity check
#
# #print(I)
# #print(D)
# #print(asizeof.asizeof(index))
#
#
#
#
#
# for i in range(200):
#     vectors2 = np.random.random(vectors.shape)
#
#     index.add(vectors2)
#     D, I = index.search(vectors2[:5], k)  # sanity check
#
#     #print(I)
#     #print(D)
#     #print(asizeof.asizeof(index))
#
# kkkk
