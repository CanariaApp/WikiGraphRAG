import pandas as pd
import atexit
from pymongo import MongoClient, UpdateOne, InsertOne
from abc import ABC, abstractmethod
from tqdm import tqdm
from typing import List


class MongoDBConnector(ABC):
    """MongoDB connector abstract class"""

    def __init__(self, connection_string: str, database_name: str) -> None:
        # connect to client
        self.client = MongoClient(connection_string)
        # database
        self.database = self.client[database_name]
        atexit.register(self.close)
        return

    def create_index(self, collection_name: str, field_name: str) -> None:
        self.database[collection_name].create_index(field_name)
        return

    def num_docs(self, collection_name: str, **kwargs) -> None:
        ndocs = self.database[collection_name].estimated_document_count()
        if kwargs.get("verbose", False):
            print(f"number of documents in {collection_name}: {ndocs}")
        return ndocs

    def drop_collection(self, collection_name: str) -> None:
        print(f"Dropping collection {collection_name}")
        self.database[collection_name].drop()
        return

    def query_df(
        self, collection_name: str, query: dict = {}, fields: dict = {}, limit: int = 0
    ) -> pd.DataFrame:
        """Query MongoDB and return DataFrame"""
        cursor = self.database[collection_name].find(query, fields).limit(limit)
        return pd.DataFrame(tqdm(cursor))

    def aggregation_pipeline_df(
        self, collection_name: str, pipeline: list
    ) -> pd.DataFrame:
        cursor = self.database[collection_name].aggregate(pipeline)
        return pd.DataFrame(tqdm(cursor))

    def close(self) -> None:
        print("Closing MongoDB connection")
        self.client.close()
        return


class MongoDBJobDB(MongoDBConnector):
    def bulk_write(self, collection_name: str, batch: list) -> None:
        if len(batch) == 0:
            return
        # batch insert
        self.database[collection_name].bulk_write(batch)
        return

    def batch_insert(self, collection_name: str, updates: List[dict]) -> None:
        # collect data
        batch = []
        for update in updates:
            batch.append(InsertOne(update))
        self.bulk_write(collection_name, batch)
        return

    def batch_update(
        self, collection_name: str, queries: List[dict], updates: List[dict]
    ) -> None:
        # collect data
        batch = []
        for query, update in zip(queries, updates):
            batch.append(UpdateOne(query, update, upsert=True))
        self.bulk_write(collection_name, batch)
        return

    def mongodb_divide_chunks(self, cursor, chunksize):
        """divide mongodb cursor c into chunks of size n"""
        chunk = []
        for i, row in enumerate(cursor):
            if i % chunksize == 0 and i > 0:
                yield chunk
                del chunk[:]
            chunk.append(row)
        yield chunk