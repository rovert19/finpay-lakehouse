from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from utils import number_correction

CATALOG = 'fintech_finpay'
BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'silver'

TRANSACTIONS_SILVER_TABLE = f'{CATALOG}.{SILVER_SCHEMA}.transactions'
TRANSACTIONS_BRONZE_TABLE = f'{CATALOG}.{BRONZE_SCHEMA}.transactions'

USERS_SILVER_TABLE = f'{CATALOG}.{SILVER_SCHEMA}.users'
USERS_BRONZE_TABLE = f'{CATALOG}.{BRONZE_SCHEMA}.users'

MERCHANTS_SILVER_TABLE = f'{CATALOG}.{SILVER_SCHEMA}.merchants'
MERCHANTS_BRONZE_TABLE = f'{CATALOG}.{BRONZE_SCHEMA}.merchants'

QUARANTINE_TABLE = f'{CATALOG}.{SILVER_SCHEMA}.quarantine'

rules_transactions = {
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_user_id": "user_id IS NOT NULL",
    "valid_merchant_id": "merchant_id IS NOT NULL",
    "valid_channel": "channel IN ('web', 'app', 'pos')",
    "valid_transaction_type": "transaction_type IN ('pago', 'reversa', 'retiro')",
    "valid_amount": "amount > 0",
    "valid_currency": "currency IN ('PEN', 'USD', 'COP', 'MXN', 'CLP', 'ARS')",
    "valid_status": "status IN ('aprobado', 'rechazado', 'pendiente')",
    "required_reference_id": """
        CASE
            WHEN transaction_type = 'reversa' THEN reference_id IS NOT NULL
            ELSE reference_id IS NULL
        END
    """
}

rules_users = {
    "valid_user_id": "user_id IS NOT NULL",
    "valid_full_name": "full_name IS NOT NULL OR full_name <> ''",
    "valid_document_id": """
        CASE
            WHEN country = 'PE' then document_id RLIKE '^[0-9]{8}$'
            ELSE document_id RLIKE '^[0-9]{8,15}$'
    """,
    "valid_email": "email LIKE '%_@__%.__%' AND email IS NOT NULL",
    "valid_phone": "phone RLIKE '^\\+[1-9][0-9]{7,14}$",
    "valid_country": "country IN ('PE', 'CO', 'MX', 'CL', 'AR')",
    "valid_segment": "segment IN ('premium', 'estandar', 'nuevo')"
}

rules_merchants = {
    "valid_merchant_id": "merchant_id IS NOT NULL",
    "valid_merchant_name": "merchant_name IS NOT NULL OR merchant_name <> ''",
    "valid_category": "category IN ('retail', 'restaurante', 'farmacia', 'supermercado', 'tecnologia', 'transporte', 'educacion', 'salud', 'entretenimiento', 'moda')",
    "valid_country": "country IN ('PE', 'CO', 'MX', 'CL', 'AR')",
    "valid_status": "status IN ('activo', 'inactivo', 'suspendido')",
    "valid_risk_level": "risk_level IN ('bajo', 'medio', 'alto') OR risk_level IS NULL"
}

quarantine_tranx_rules = "NOT({0})".format(" AND ".join(rules_transactions.values()))
quarantine_users_rules = "NOT({0})".format(" AND ".join(rules_users.values()))
quarantine_merchants_rules = "NOT({0})".format(" AND ".join(rules_merchants.values()))

number_correction_udf = F.udf(number_correction, StringType())

## Processing Transactions
@dp.temporary_view(
    name='tmp_validation_tranx'
)
@dp.expect_all(rules_transactions)
def validation_and_clean_transactions():
    return (
      spark.readStream.table(TRANSACTIONS_BRONZE_TABLE)
        .select(
            F.col('transaction_id'),
            F.col('user_id'),
            F.col('merchant_id'),
            F.lower('channel').alias('channel'),
            F.lower('transaction_type').alias('transaction_type'),
            number_correction_udf(F.col('amount')).cast('decimal(10,2)').alias('amount'),
            F.when(F.upper('currency') == 'SOL' or F.upper('currency') == 'SOLES', 'PEN')
                .when(F.upper('currency') == 'US$', 'USD')
                .otherwise(F.upper('currency')).alias('currency'),
            F.when(F.col('transaction_date').contains('/'), F.to_date(F.col('transaction_date'), 'dd/MM/yyyy'))
                .when(F.col('transaction_date').contains('-'), F.to_date(F.col('transaction_date'), 'yyyy-MM-dd'))
                .alias('transaction_date'),
            F.lower('status').alias('status'),
            F.col('reference_id'),
            F.col('ingestion_at'),
            F.col('source_file')
        )
        .withColumn("is_quarantined", F.expr(quarantine_tranx_rules))
        .withColumn("rejected_by", F.concat_ws(';', 
                                *[F.when(F.expr("NOT{0}".format(v)), k).otherwise("") 
                                  for k,v in rules_transactions.items()]
                            )
                    )
    )


@dp.table(
    name=TRANSACTIONS_SILVER_TABLE
)
def transactions_silver():
    return (
        spark.readStream.table('tmp_validation_tranx')
            .filter("is_quarantined = false")
            .drop(["is_quarantined", "rejected_by"])  
        )


## Processing users
@dp.temporary_view(
    name='tmp_validation_users'
)
@dp.expect_all(rules_users)
def validation_and_clean_users():
    return (
        spark.readStream.table(USERS_BRONZE_TABLE)
            .select(
                F.col('user_id'),
                F.col('full_name'),
                F.col('document_id'),
                F.col('email'),
                F.col('phone'),
                F.upper('country').alias('country'),
                F.trim(F.lower('segment')).alias('segment'),
                F.when(F.col('registration_date').contains('/'), F.to_date(F.col('registration_date'), 'dd/MM/yyyy'))
                    .when(F.col('registration_date').contains('-'), F.to_date(F.col('registration_date'), 'yyyy-MM-dd'))
                    .alias('registration_date'),
                F.col('ingestion_at'),
                F.col('source_file')
            )
            .withColumn("is_quarantined", F.expr(quarantine_users_rules))
            .withColumn("rejected_by", F.concat_ws(';', 
                                *[F.when(F.expr("NOT{0}".format(v)), k).otherwise("") 
                                  for k,v in rules_users.items()]
                            )
                    )
    )


@dp.table(
    name=USERS_SILVER_TABLE
)
def users_silver():
    return (
        spark.readStream.table('tmp_validation_users')
            .filter("is_quarantined = false")
            .drop(["is_quarantined", "rejected_by"])  
        )


## Processing merchants
@dp.temporary_view(
    name='tmp_validation_merchants'
)
@dp.expect_all(rules_merchants)
def merchants_silver():
    return (
        spark.readStream.table(MERCHANTS_BRONZE_TABLE)
            .select(
                F.col('merchant_id'),
                F.col('merchant_name'),
                F.lower('category').alias('category'),
                F.upper('country').alias('country'),
                F.when(F.col('affiliation_date').contains('/'), F.to_date(F.col('affiliation_date'), 'dd/MM/yyyy'))
                    .when(F.col('affiliation_date').contains('-'), F.to_date(F.col('affiliation_date'), 'yyyy-MM-dd'))
                    .alias('affiliation_date'),
                F.lower('status').alias('status'),
                F.lower('risk_level').alias('risk_level'),
                F.col('ingestion_at'),
                F.col('source_file')
            )
            .withColumn("is_quarantined", F.expr(quarantine_merchants_rules))
            .withColumn("rejected_by", F.concat_ws(';', 
                                *[F.when(F.expr("NOT{0}".format(v)), k).otherwise("") 
                                  for k,v in rules_merchants.items()]
                            )
                    )
    )


@dp.table(
    name=MERCHANTS_SILVER_TABLE
)
def merchants_silver():
    return (
        spark.readStream.table('tmp_validation_merchants')
            .filter("is_quarantined = false")
            .drop(["is_quarantined", "rejected_by"])  
        )


## Ingestion quarantine table

@dp.table(
    name= QUARANTINE_TABLE
)
def quarantine_silver():
    tranxs_quarantine = spark.readStream.table('tmp_validation_tranx')
        .filter("is_quarantined = true")
        .drop("is_quarantined")
        .select(
            F.concat_ws('|', F.col('transaction_id'), F.col('user_id'), F.col('merchant_id'), 
                        F.col('channel'), F.col('transaction_type'), F.col('amount'), F.col('currency'), 
                        F.col('transaction_date'), F.col('status'), F.col('reference_id')
                        )
                .alias('payload'),
            F.col('ingestion_at').alias('procesing_at'),
            F.col('source_file'),
            F.col('rejected_by')
        )

    users_quarantine = spark.readStream.table('tmp_validation_users')
        .filter("is_quarantined = true")
        .drop("is_quarantined")
        .select(
            F.concat_ws('|', F.col('user_id'), F.col('full_name'), F.col('document_id'), F.col('email'), 
                        F.col('phone'), F.col('country'), F.col('segment'), F.col('registration_date'))
                .alias('payload'),
            F.col('ingestion_at').alias('procesing_at'),
            F.col('source_file'),
            F.col('rejected_by')
        )

    merchants_quarantine = spark.readStream.table('tmp_validation_merchants')
        .filter("is_quarantined = true")
        .drop("is_quarantined")
        .select(
            F.concat_ws('|', F.col('merchant_id'), F.col('merchant_name'), F.col('category'), 
                        F.col('country'), F.col('affiliation_date'), F.col('status'), F.col('risk_level'))
                .alias('payload'),
            F.col('ingestion_at').alias('procesing_at'),
            F.col('source_file'),
            F.col('rejected_by')
        ) 

  
    return (
        tranxs_quarantine
            .union(users_quarantine)
            .union(merchants_quarantine)
    )