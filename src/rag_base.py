import numpy as np
import multiprocessing as mp
import time
import pickle

from random_walk import RandomWalk, Link

def direct_search(query,embeddingfunc_direct_search,index_path, k_best=3):
    #Load pre-generated index
    with open(index_path, 'rb') as f:
        temp = pickle.load(f)
    vector_index = temp[0]
    vector_id_list = temp[1]
    #Do vector search
    start_time = time.time()
    vec = embeddingfunc_direct_search(query)
    D, I = vector_index.search(np.reshape(vec,(1,1024)), k_best)
    list_of_k_best_nodes = [vector_id_list[i] for i in I[0]]
    return list_of_k_best_nodes, (time.time() - start_time)


def get_paths_from_node(query,start_node, embeddingfunc, QueryDB, n_walks=3, max_steps=5, num_threads=1):
    #Initiate random walks from given startpoint using parallel processing
    random_seeds = np.random.randint(1, 65536, size=n_walks)
    # with mp.Pool(num_threads) as pool:
    #     results = pool.starmap(
    #         RandomWalk,
    #         [(query, start_node, embeddingfunc, QueryDB, max_steps, random_seed) for random_seed in random_seeds]
    #     )
    results = [RandomWalk(query, start_node, embeddingfunc, QueryDB, max_steps, random_seed) for random_seed in random_seeds]
    return results

def deduplicate_doc_list(doc_list):
    #Deduplicate docs while preserving order
    doc_id_included = {}
    dedup_doc_list = []
    for doc in doc_list:
        doc_id = str(doc['id'][0])
        if doc_id_included.get(doc_id) is None:
            dedup_doc_list.append(doc)
            doc_id_included[doc_id] = True
    return dedup_doc_list


def reranker(paths):
    #Reorder paths and documents based on ranking method

    #Simple reranking based on similarity score of the full context
    paths.sort(key = lambda p : np.max(p.scores))

    #Get ordered document lists from paths
    docs = []
    for path in paths:
        docs += [link.target for link in path.links_traversed]
    #Deduplicate document list while preserving order
    dedup_docs = deduplicate_doc_list(docs)

    return dedup_docs


def retriever(query, embeddingfunc,embeddingfunc_direct_search, QueryDB, index_path, n_walks=1, max_steps=3, k_best=3):
    #Get starting documents with direct search
    start_ids, vector_search_time = direct_search(query, embeddingfunc_direct_search, index_path, k_best)
    # Get corresponding entries from DB

    basic_docs = QueryDB.get_index(start_ids)
    # basic_docs = []
    # df = QueryDB.get_index(start_ids)
    # for _, row in df.iterrows():
    #     basic_docs.append(row)


    #Find most relevant path through document links for each starting node
    start_time = time.time()
    paths = []
    for start_node in basic_docs:
        paths += get_paths_from_node(query,start_node,embeddingfunc, QueryDB, n_walks=n_walks, max_steps=max_steps)
    random_walk_time = (time.time() - start_time)

    #Rerank documents
    ordered_docs = reranker(paths)
    #Return retrieved, ordered documents
    return basic_docs, ordered_docs, vector_search_time, random_walk_time
