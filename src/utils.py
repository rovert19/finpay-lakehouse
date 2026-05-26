
import json
import re 
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType

VOLUME = '/Volumes/fintech_finpay/default/vol_landing'
METADATA_FOLDER = f'{VOLUME}/metadata'

def get_metadata_json():
  with open(f'{METADATA_FOLDER}/ingestion_archetypes.json') as f:
    return json.load(f)

def get_archetype_info(source_name):
  archetype_metadata = get_metadata_json()
  for archetype in archetype_metadata:
      if archetype['source_name'] == source_name and archetype['active'] == True:
          return archetype
  return None

USER_SCHEMA = StructType([
  StructField("user_id", StringType()),
  StructField("full_name", StringType()),
  StructField("document_id", StringType()),
  StructField("email", StringType()),
  StructField("phone", StringType()),
  StructField("country", StringType()),
  StructField("segment", StringType()),
  StructField("registration_date", StringType())
])

MERCHANT_SCHEMA = StructType([
  StructField("merchant_id", StringType()),
  StructField("merchant_name", StringType()),
  StructField("category", StringType()),
  StructField("country", StringType()),
  StructField("affiliation_date", StringType()),
  StructField("status", StringType()),
  StructField("risk_level", StringType()) 
])

TRANSACTION_SCHEMA = StructType([
  StructField("transaction_id", StringType()),
  StructField("user_id", StringType()),
  StructField("merchant_id", StringType()),
  StructField("channel", StringType()),
  StructField("transaction_type", StringType()),
  StructField("amount", StringType()),
  StructField("currency", StringType()),
  StructField("transaction_date", StringType()),
  StructField("status", StringType()),
  StructField("reference_id", StringType())
])

def get_schemas():
  return {
    'users': USER_SCHEMA,
    'merchants': MERCHANT_SCHEMA,
    'transactions': TRANSACTION_SCHEMA
  }

  if re.match(r"^.*\..*,.*$", texto)

  def number_correction(amount):
    if re.fullmatch(r".*.,..*", amount):
      return (
          amount.replace('.', '')
              .replace(',', '.')
      )
    return amount