#Class to create vector a search index for a large dataset

import pickle
import faiss
import numpy as np

class VectorSearch:
    """
    Inputs:
        ncentroid : Number of centroids for index
        nsubquantizers : Number of subquantizers for index
        nbits : specifies that each sub-vector is encoded as nbits bits
    """
    def __init__(self,
                 vector_dim : int, ncentroids : int = 5000, nsubquantizers : int = 128, nbits : int = 8):
        # Init index
        self.vector_dim = vector_dim #dimensionality of the vectors
        self.quantizer = faiss.IndexFlatL2(self.vector_dim )
        self.index = faiss.IndexIVFPQ(self.quantizer, vector_dim, ncentroids, nsubquantizers, nbits)
        self.id_list = []

    def train(self, representative_sample_ids : list, representative_sample_vectors : np.ndarray ):
        # train index on representative sample
        self.index.train(representative_sample_vectors) #sample should be a np.array with shape of (n_batch,vector_dim)
        self.index.add(representative_sample_vectors)
        #Store index to id match list
        self.id_list=representative_sample_ids

    def add_batch(self, batch_ids : list, batch_vectors : np.ndarray):
        # Add to index
        self.index.add(batch_vectors) # batch should be a np.array with shape of (n_batch,vector_dim)
        # keep ids
        self.id_list += batch_ids

    def remove_batch(self, batch_ids : list, batch_vectors: np.ndarray):
        #Remove from index
        self.index.remove_ids(batch_vectors) # batch should be a np.array with shape of (n_batch,vector_dim)
        #I think self.id_list shouldn't be changed

    def search(self, query_vec : np.ndarray, k_best : int = 3):
        # Do vector search for k_best nearest
        D, I = self.index.search(np.reshape(query_vec, (1, self.vector_dim)), k_best) #returns Distance and Index(needs to be converted to id)
        list_of_k_best_indices = [self.id_list[i] for i in I[0]]
        return list_of_k_best_indices

    def save_index(self, filename : str):
        # Save index data
        with open(filename, 'wb') as f:
            pickle.dump([self.index, self.id_list], f)
        #print("Index saved to " + filename)

    def load_index(self, filename : str):
        with open(filename, 'rb') as f:
            temp = pickle.load(f)
        self.index = temp[0]
        self.id_list = temp[1]
        #print("Index loaded from " + filename)
