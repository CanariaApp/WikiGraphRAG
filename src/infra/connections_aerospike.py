from time import time
import atexit
import aerospike
from aerospike import exception as ex  # type:ignore
from abc import ABC
from tqdm import tqdm
from typing import List
from os import environ


class AerospikeConnector(ABC):
    """Aerospike connector abstract class"""

    def __init__(
        self,
        port: int = 3001,
        host: str = "127.0.0.1",
        timeout: int = 10000,
    ) -> None:
        # configuration
        self.config = {
            "hosts": [(host, port)],
            "policies": {"timeout": timeout},  # milliseconds
        }
        # connect to client
        self.client = aerospike.Client(self.config)
        self.node_name = self.client.get_node_names()[0]["node_name"]

        atexit.register(self.close)
        return

    def create_index(
        self,
        namespace: str,
        set_name: str,
        bin_name: str,
        index_type: str,
    ) -> None:
        """Create index"""
        index_name = f"index_{namespace}_{set_name}_{bin_name}_{index_type}"
        try:
            if index_type == "string":
                self.client.index_string_create(
                    namespace, set_name, bin_name, index_name
                )
                print(f"Index '{index_name}' created successfully.")
            elif index_type == "integer":
                self.client.index_integer_create(
                    namespace, set_name, bin_name, index_name
                )
                print(f"Index '{index_name}' created successfully.")
            else:
                print(f"Invalid index type: {index_type}")
        except ex.IndexFoundError:
            print(f"Index '{index_name}' already exists.")
        except Exception as e:
            print(f"Failed to create index: {e}")

    def delete_index(
        self,
        namespace: str,
        set_name: str,
        bin_name: str,
        index_type: str,
    ) -> None:
        """Delete index"""
        index_name = f"index_{namespace}_{set_name}_{bin_name}_{index_type}"
        try:
            self.client.index_remove(namespace, index_name)
            print(f"Index '{index_name}' deleted successfully.")
        except ex.IndexNotFound:
            print(f"Index '{index_name}' not found.")
        except Exception as e:
            print(f"Failed to delete index: {e}")

    def query(
        self,
        namespace: str,
        set_name: str,
        bin_name: str,
        index_type: str,
        expression,
    ) -> List[str]:
        # Ensure index exists
        try:
            self.create_index(namespace, set_name, bin_name, index_type)
        except Exception as e:
            print(f"Error while ensuring index: {e}")
            return []

        keys = []
        try:
            query = self.client.query(namespace, set_name)
            query.select()
            query.where(expression)

            for record in tqdm(query.results(), disable=False):
                keys.append(record[2])  # Assuming record[2] is the key

        except ex.IndexNotFound:
            print(f"Index not found for bin '{bin_name}'.")
        except Exception as e:
            print(f"Query failed: {e}")

        return keys

    def get_all(self, namespace: str, set_name: str, **kwargs) -> List[str]:
        """Get all keys in a set"""
        keys = []
        query = self.client.query(namespace, set_name)
        query.select()
        for record in tqdm(query.results(), disable=not kwargs.get("verbose", False)):
            keys.append(record[2])
        return keys

    def put(self, namespace: str, set_name: str, key: tuple, value: dict) -> None:
        try:
            # Using the instance's client attribute to perform the put operation.
            self.client.put((namespace, set_name, key), value)
        except Exception as e:
            # Printing out the key and exception message to standard output.
            # In a production environment, consider logging this instead.
            print("key:", key)
            print(e)
        return

    def read(
        self,
        namespace: str,
        set_name: str,
        key: str,
        **kwargs,
    ):
        try:
            return self.client.get((namespace, set_name, str(key)))
        except Exception as e:
            if kwargs.get("verbose", False):
                print("key:", key)
                print(e)
            return None

    def batch_insert(
        self,
        namespace: str,
        set_name: str,
        key_list: list,
        value_list: list,
        **kwargs,
    ):
        ti = time()
        counter = 0
        for key_entry, value in tqdm(zip(key_list, value_list)):
            key = (namespace, set_name, str(key_entry))
            counter += 1

            try:
                self.client.put(key, value, policy={"key": aerospike.POLICY_KEY_SEND})
            except Exception as e:
                print("key:", key)
                print(e)
                break

            if counter % 10000 == 0:
                self.client.reconnect()

        if kwargs.get("verbose", False):
            print(f"inserted {len(key_list)} keys in {time()-ti:2.2f} s")

        return

    @staticmethod
    def extract(x, field: str) -> str:
        if x is None:
            return None
        if x.record is None:
            return None
        if x.record[2] is None:
            return None
        return x.record[2].get(field, None)

    def batch_read(
        self,
        key_list: list,
        field: str,
        namespace: str = "test",
        set_name: str = "benchmark",
    ):
        return [
            AerospikeConnector.extract(_d, field)
            for _d in self.batch_records_read(
                key_list, namespace, set_name
            ).batch_records
        ]

    def batch_records_read(
        self,
        batch_key: List[tuple],
        namespace: str = "test",
        set_name: str = "benchmark",
    ):
        """"""
        pk_list = []
        for key in batch_key:
            key = (namespace, set_name, str(key))
            pk_list.append(key)

        return self.client.batch_read(pk_list)

    def num_docs_namespace(self, namespace) -> int:
        """Print number of documents in database"""
        info = self.client.info_single_node(f"namespace/{namespace}", self.node_name)
        stats = dict(item.split("=") for item in info.split(";"))
        num_docs = stats.get("objects")
        if stats.get("objects") is not None:
            print(f"Number of documents in '{namespace}': {num_docs}")
        return num_docs

    def num_docs_set(self, namespace, set) -> int:
        """Print number of documents in a collection"""
        query = f"sets/{namespace}/{set}"
        info = self.client.info_single_node(
            f"sets/{namespace}/{set}", self.node_name
        ).split(query)[1][1:]
        stats = dict(item.split("=") for item in info.split(":"))
        num_docs = stats.get("objects")
        if stats.get("objects") is not None:
            print(f"Number of documents in '{namespace}': {num_docs}")
        return num_docs

    def drop_db(self, namespace: str, set: str, nanos=0) -> None:
        """Drop database"""
        self.client.truncate(namespace, set, nanos)
        return

    def close(self) -> None:
        print("Closing Aerospike connection")
        if self.client.is_connected():
            self.client.close()
        return
