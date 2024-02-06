import pyarrow as pa
import pyarrow.parquet as pq
from os import path
import os
from pandas import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/redit.json'

bucket_name = 'de-course-411610-demo-bucket'
project_id = 'de-course-411610'

table_name = 'green_taxi'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage_partitioned(df: DataFrame, **kwargs) -> None:
#    df['tpep_pickup_date'] = df['tpep_pickup_datetime'].dt.date
   table = pa.Table.from_pandas(df)

   gcs = pa.fs.GcsFileSystem()
   pq.write_to_dataset(
    table,
    root_path=root_path,
    partition_cols=['lpep_pickup_date'],
    filesystem = gcs
   )
