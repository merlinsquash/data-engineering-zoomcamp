import argparse
import gzip
import os
import shutil
from time import time

import pandas as pd
import pyarrow.parquet as pq
import wget
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

db_conn_str = "{db}://{username}:{password}@{host}:{port}/{database_name}"

args_with_desc = {
    "db": "database name - mysql,postgresql etc.",
    "user": "user name for postgres",
    "password": "password for postgres",
    "host": "host for postgres",
    "port": "port for postgres",
    "database_name": "postgres db name",
    "table_name": "table where file will be ingested",
    "url": "download URL of the file to be ingested",
}


class IngestTaxiData:
    def __init__(self, params):
        self.params = params

        print("Step 1 : Download CSV from URL")
        print(params.url)
        self.file_name = wget.download(params.url)

        if self.file_name.endswith(".gz"):
            print("Found compressed file - to extract")
            with gzip.open(self.file_name, 'rb') as f_in:
                actual_file_name = self.file_name.rsplit(".", 1)[0]
                with open(actual_file_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                self.file_name = actual_file_name

        print(self.file_name)

        # Step 1A Check the file has been downloaded and is not empty

        if os.path.exists(self.file_name) and os.stat(self.file_name).st_size:
            print("Step 1 Completed : File downloaded successfully")
        else:
            print("Step 1 Failed : File not downloaded. Aborting Pipeline")
            return

        print("Step 2 Connect to DB")
        self.engine = create_engine(
            db_conn_str.format(db=params.db, username=params.user, password=params.password, host=params.host,
                               port=params.port, database_name=params.database_name)
        )

        try:
            self.engine.connect()
        except SQLAlchemyError as err:
            print("Step 2 Failed due to SQLAlchemyError", err.__cause__)
            return
        else:
            print("Step 2 Completed : DB Connection sucessful, proceeding with ingestion")

    def ingest_runner(self):
        """
        Function will read the downloaded file and ingest into table
        """

        # Load file but in chunks
        # TODO Check how to decide on an optimal chunksize
        if self.file_name.endswith(".csv"):
            chunk_iterator = pd.read_csv(self.file_name, chunksize=100000)
        elif self.file_name.endswith(".parquet"):
            chunk_iterator = pq.ParquetFile(self.file_name).iter_batches(batch_size=100000)
        #TODO Atomic Transactions
        for each_chunk in chunk_iterator:
            t_start = time()
            if self.file_name.endswith(".parquet"):
                each_chunk = each_chunk.to_pandas()
            each_chunk.lpep_pickup_datetime = pd.to_datetime(each_chunk.lpep_pickup_datetime)
            each_chunk.lpep_dropoff_datetime = pd.to_datetime(each_chunk.lpep_dropoff_datetime)
            each_chunk.to_sql(name=self.params.table_name, con=self.engine, if_exists='append', index=False)
            print("inserted chunk took %.3f second" % (time() - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Taxi Data to Table')

    for arg, arg_desc in args_with_desc.items():
        parser.add_argument(arg, help=arg_desc)

    args = parser.parse_args()
    IngestTaxiData(args).ingest_runner()
