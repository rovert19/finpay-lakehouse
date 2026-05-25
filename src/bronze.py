from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils import get_archetype_info

TRANSACTIONS_SOURCE = 'transactions'
USERS_SOURCE = 'users'
MERCHANTS_SOURCE = 'merchants'

sources = [USERS_SOURCE, TRANSACTIONS_SOURCE, MERCHANTS_SOURCE]

def load_source(source_name):
    metadata = get_archetype_info(source_name)

    @dp.table(
        name= metadata['target_table'],
        partition_cols= [metadata['partition_by']]
    )
    def bronze_table():
        return (
            spark.readStream
                .format('cloudFiles')
                .option('cloudFiles.format', metadata['file_format'] if metadata['file_format'] != 'text' else "csv")
                .option('cloudFiles.schemaLocation', metadata['schema_location'])
                .option('delimiter', metadata['delimiter'])
                .option('header', metadata['header'])
                .option('multiline', metadata['multiline'])
                .load(metadata['source_path'])
                .withColumn('ingestion_at', F.current_timestamp())
                .withColumn('source_file', F.lit("_metadata.file_name"))
        )


for src in sources:
    load_source(src)

