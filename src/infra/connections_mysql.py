import os
import tempfile
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
            self.cnx = mysql.connector.connect(
                user=self.user, 
                database=self.database, 
                password=self.password, 
                port=self.port,
                allow_local_infile=True,
            )
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
    def create_index(self, table_name: str, columns: list, index_lengths=None, **kwargs) -> None:
        """
        Create an index on the specified table and columns, with optional prefix lengths.
        """
        unique = kwargs.get("unique", False)
        verbose = kwargs.get("verbose", False)

        # Build the index name based on the table name and columns
        index_name = f"idx_{table_name}_{'_'.join(columns)}"

        # Construct the column definitions for the index
        if index_lengths and len(index_lengths) == len(columns):
            # Apply prefix lengths if provided
            columns_with_lengths = [f"{col}({length})" for col, length in zip(columns, index_lengths)]
        else:
            # No prefix lengths specified, use entire columns
            columns_with_lengths = columns

        # Construct the CREATE INDEX query
        query = f"CREATE {'UNIQUE' if unique else ''} INDEX {index_name} ON {table_name} ({', '.join(columns_with_lengths)});"

        # Execute the query
        cursor = self.cnx.cursor()
        try:
            if verbose:
                print(f"Creating index '{index_name}' on table '{table_name}' for columns: {', '.join(columns_with_lengths)}")
            cursor.execute(query)
            if verbose:
                print(f"Index '{index_name}' created successfully.")
        except mysql.connector.Error as err:
            if verbose:
                print(f"Failed creating index on table {table_name}: {err}")
        finally:
            cursor.close()

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
        """
        Optimized insertion of a DataFrame into MySQL using `LOAD DATA INFILE`.
        """
        # Handle optional arguments
        primary_keys = kwargs.get("primary_keys", None)
        foreign_keys = kwargs.get("foreign_keys", None)
        verbose = kwargs.get("verbose", False)

        # Preprocess the DataFrame: Drop rows with NaNs in primary/foreign keys and remove duplicates
        if primary_keys:
            df = df.dropna(subset=primary_keys)
        if foreign_keys:
            for col, _ in foreign_keys.items():
                df = df.dropna(subset=[col])
        if primary_keys:
            df = df.drop_duplicates(subset=primary_keys)
        
        # Replace NaN values with None to avoid MySQL insert issues
        df = df.replace({np.nan: None})

        # Convert DataFrame to a CSV format for `LOAD DATA INFILE`
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.csv') as temp_file:
            # Save DataFrame as CSV in the temp file
            temp_file_path = temp_file.name
            df.to_csv(temp_file_path, sep=',', index=False, header=False)

        # Load the CSV into the MySQL table using `LOAD DATA INFILE`
        cursor = self.cnx.cursor()
        try:
            # Construct the `LOAD DATA INFILE` query
            query = f"""
            LOAD DATA LOCAL INFILE '{temp_file_path}'
            INTO TABLE {table_name}
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            ({", ".join(df.columns)});
            """

            # Execute the query
            cursor.execute(query)
            self.cnx.commit()
            if verbose:
                print(f"Successfully inserted {len(df)} rows into `{table_name}` using LOAD DATA INFILE.")
        except mysql.connector.Error as err:
            if verbose:
                print(f"Failed to load data into `{table_name}`: {err}")
        finally:
            cursor.close()
            # Delete the temporary file
            os.remove(temp_file_path)
    
    @classmethod
    def return_dataframe(self, query: str) -> pd.DataFrame:
        cursor = self.cnx.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        return pd.DataFrame(data, columns=columns)
    