import atexit
from abc import ABC, abstractmethod
from time import sleep
import numpy as np
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from typing import List, Dict
from tqdm import tqdm

class MySQLConnector(ABC):
    """MySQL connector abstract class"""

    @classmethod
    def __init__(self, user: str, database: str, password: str, port: int=3306, **kwargs) -> None:
        self.user = user
        self.database = database
        self.password = password
        self.port = port
        self.connect()

        verbose = kwargs.get("verbose", False)
        atexit.register(self.close, verbose=verbose)
        return
    
    @classmethod
    def connect(self):
        try:
            self.cnx = mysql.connector.connect(user=self.user, database=self.database, password=self.password, port=self.port)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        return
    
    @classmethod
    def close(self, **kwargs) -> None:
        verbose = kwargs.get("verbose", False)
        if verbose:
            print("Closing MySQL connection")
        self.cnx.close()
        return
    
    @classmethod
    def get_all_tables(self, **kwargs) -> List[str]:
        cursor = self.cnx.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        if kwargs.get("verbose", False):
            print(f"Found {len(tables)} tables in the database.")
            for table in tables:
                print(table)
        return tables
    
    @classmethod
    def create_table(
        self, 
        table_name: str, 
        columns: Dict[str, str], 
        **kwargs,
    ) -> None:
        
        primary_keys = kwargs.get("primary_keys", None)
        foreign_keys = kwargs.get("foreign_keys", None)
        verbose = kwargs.get("verbose", False)

        cursor = self.cnx.cursor()

        # build the columns definition part of the query
        columns_definition = ", ".join([f"{col} {datatype}" for col, datatype in columns.items()])

        # initialize the query with the column definitions
        query = f"CREATE TABLE {table_name} (\n{columns_definition}"

        # add primary key constraint if provided
        if primary_keys:
            primary_keys = ", ".join(primary_keys)
            query += f",\nPRIMARY KEY ({primary_keys})"

        # add foreign key constraints if provided
        if foreign_keys:
            for col, ref in foreign_keys.items():
                query += f",\nFOREIGN KEY ({col}) REFERENCES {ref}"

        # close the query with the correct SQL syntax
        query += "\n);"

        # execute the query
        try:
            cursor.execute(query)
        except mysql.connector.Error as err:
            if verbose:
                print(f"Failed creating table {table_name}: {err}")
        finally:
            cursor.close()
    
    @classmethod
    def create_index(
        self, 
        table_name: str, 
        columns: List[str], 
        **kwargs,
    ) -> None:
        
        unique = kwargs.get("unique", False)
        verbose = kwargs.get("verbose", False)
        cursor = self.cnx.cursor()

        # build the columns definition part of the query
        columns = ", ".join(columns)

        # initialize the query with the column definitions
        query = f"CREATE {'UNIQUE' if unique else ''} INDEX idx_{table_name}_{columns} ON {table_name} ({columns});"

        # execute the query
        try:
            cursor.execute(query)
        except mysql.connector.Error as err:
            if verbose:
                print(f"Failed creating index on table {table_name}: {err}")
        finally:
            cursor.close()
        return

    @classmethod
    def show_indexes(self, table_name: str) -> None:
        cursor = self.cnx.cursor()
        query = f"SHOW INDEX FROM {table_name}"
        cursor.execute(query)
        indexes = cursor.fetchall()
        cursor.close()
        for index in indexes:
            print(index)

    @classmethod
    def delete_index(self, table_name: str, **kwargs) -> None:
        verbose = kwargs.get("verbose", False)
        cursor = self.cnx.cursor()
        query = f"DROP INDEX idx_{table_name}"

        # execute the query
        try:
            cursor.execute(query)
            if verbose:
                print(f"Index idx_{table_name} has been deleted.")
        except mysql.connector.Error as err:
            if verbose:
                print(f"Failed to delete index idx_{table_name}: {err}")
        finally:
            cursor.close()
        return

    @classmethod
    def delete_table(self, table_name: str, **kwargs) -> None:
        verbose = kwargs.get("verbose", False)
        cursor = self.cnx.cursor()
        query = f"DROP TABLE IF EXISTS {table_name}"

        # execute the query
        try:
            cursor.execute(query)
            if verbose:
                print(f"Table {table_name} has been deleted.")
        except mysql.connector.Error as err:
            if verbose:
                print(f"Failed to delete table {table_name}: {err}")
        finally:
            cursor.close()
        return
    
    @classmethod
    def insert_dataframe(
        self, 
        table_name: str, 
        df: pd.DataFrame,
        **kwargs,
    ) -> None:
        
        primary_keys = kwargs.get("primary_keys", None)
        foreign_keys = kwargs.get("foreign_keys", None)
        batch_size = kwargs.get("batch_size", 1000)
        verbose = kwargs.get("verbose", False)
        max_retries = kwargs.get("max_retries", 3)

        # filter out rows where any primary or foreign key is None
        if primary_keys:
            df = df.dropna(subset=primary_keys)
        if foreign_keys:
            for col, ref in foreign_keys.items():
                df = df.dropna(subset=[col])
        df = df.replace({np.nan: None})
        # remove any rows with duplicate primary
        if primary_keys:
            df = df.drop_duplicates(subset=primary_keys)
        
        df = df.replace({np.nan: None})
        cursor = self.cnx.cursor()
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s" for _ in range(len(df.columns))])

        # query
        query = f"""
        INSERT IGNORE INTO {table_name} ({columns})
        VALUES ({placeholders})
        """
        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # execute the query
        try:
            for i in tqdm(range(0, len(data), batch_size), disable=not verbose):
                batch = data[i:i+batch_size]
                attempts = 0
                while attempts < max_retries:
                    try:
                        cursor.executemany(query, batch)
                        self.cnx.commit()
                        sleep(0.05 + np.random.rand() * 0.025)  # sleep to avoid overloading the server
                        break  # exit retry loop if successful
                    except mysql.connector.Error as err:
                        if err.errno == 1213:  # deadlock found
                            attempts += 1
                            if verbose:
                                print(f"Deadlock detected. Retry {attempts}/{max_retries}")
                            if attempts == max_retries:
                                raise  # eethrow the exception if max retries are exceeded
                        else:
                            raise  # eethrow any other exceptions
        except mysql.connector.Error as err:
            if verbose:
                print(f"Failed to insert data into {table_name}: {err}")
        finally:
            cursor.close()
        return
    
    @classmethod
    def return_dataframe(self, query: str) -> pd.DataFrame:
        cursor = self.cnx.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        return pd.DataFrame(data, columns=columns)
    