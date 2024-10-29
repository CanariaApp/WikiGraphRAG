import numpy as np
import multiprocessing as mp
from random_walk import RandomWalk, Link

def direct_search(query, k_best=3):
    return list_of_k_best_nodes

def get_paths_from_node(query,startpoint, n_walks=10, max_steps=5, num_threads=1):
    #Initiate random walks from given startpoint using parallel processing
    with mp.Pool(num_threads) as pool:
        random_seeds = np.random.randint(1,65536, size = n_walks)
        results = pool.starmap(
            RandomWalk,
            [(query, startpoint, max_steps, random_seed) for random_seed in random_seeds]
        )

def deduplicate_doc_list(doc_list):
    #Deduplicate docs while preserving order
    doc_id_included = {}
    dedup_doc_list = []
    for doc in doc_list:
        if doc_id_included.get(doc['id']) is None:
            dedup_doc_list.append(doc)
            doc_id_included[doc['id']] = True
    return dedup_doc_list


def reranker(paths):
    #Reorder paths and documents based on ranking method

    #Simple reranking based on similarity score of the full context
    ranked_paths = paths.sort(key = lambda p : p.scores[-1])

    #Get ordered document lists from paths
    docs = []
    for path in ranked_paths:
        docs += [link.target for link in path.links_traversed]
    #Deduplicate document list while preserving order
    dedup_docs = deduplicate_doc_list(docs)

    return dedup_docs


def retriever(query, n_walks=3, max_steps=3, k_best=3):
    #Get starting documents with direct search
    list_of_startpoints = direct_search(query, k_best)

    #Find most relevant path through document links for each starting node
    paths = []
    for startpoint in list_of_startpoints:
        paths += get_paths_from_node(query,startpoint, n_walks=n_walks, max_steps=max_steps)

    #Rerank documents
    ordered_docs = reranker(paths)
    #Return retrieved, ordered documents
    return ordered_docs
