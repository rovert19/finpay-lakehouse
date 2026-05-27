
import json
import re 
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType

VOLUME = '/Volumes/fintech_finpay/default/vol_landing'
METADATA_FOLDER = f'{VOLUME}/metadata'

CATALOG = 'fintech_finpay'
BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'silver'
GOLD_SCHEMA = 'gold'

TRANSACTIONS_GOLD_TABLE = f'{CATALOG}.{GOLD_SCHEMA}.transactions'
TRANSACTIONS_SILVER_TABLE = f'{CATALOG}.{SILVER_SCHEMA}.transactions'
TRANSACTIONS_BRONZE_TABLE = f'{CATALOG}.{BRONZE_SCHEMA}.transactions'

USERS_GOLD_TABLE = f'{CATALOG}.{GOLD_SCHEMA}.users'
USERS_SILVER_TABLE = f'{CATALOG}.{SILVER_SCHEMA}.users'
USERS_BRONZE_TABLE = f'{CATALOG}.{BRONZE_SCHEMA}.users'

MERCHANTS_GOLD_TABLE = f'{CATALOG}.{GOLD_SCHEMA}.merchants'
MERCHANTS_SILVER_TABLE = f'{CATALOG}.{SILVER_SCHEMA}.merchants'
MERCHANTS_BRONZE_TABLE = f'{CATALOG}.{BRONZE_SCHEMA}.merchants'

QUARANTINE_TABLE = f'{CATALOG}.{SILVER_SCHEMA}.quarantine'


def get_metadata_json():
  with open(f'{METADATA_FOLDER}/ingestion_archetypes.json') as f:
    return json.load(f)


def get_archetype_info(source_name):
  archetype_metadata = get_metadata_json()
  for archetype in archetype_metadata:
      if archetype['source_name'] == source_name and archetype['active'] == True:
          return archetype
  return None

@udf(StringType())
def cleaning_amount(amount):
  if amount != None and  re.match(r".*.,..*", amount):
    return (
        amount.replace('.', '')
            .replace(',', '.')
    )
  return amount

# @dp.table(
#     name=MERCHANTS_SILVER_TABLE
# )
# def merchants_silver():
#     return (
#         spark.readStream.table('tmp_validation_merchants')
#             .filter("is_quarantined = false")
#             .drop("is_quarantined")
#             .drop("rejected_by")  
#         )