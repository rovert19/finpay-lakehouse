from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils import (
    cleaning_amount, 
    TRANSACTIONS_SILVER_TABLE, 
    TRANSACTIONS_GOLD_TABLE, 
    USERS_SILVER_TABLE,
    USERS_GOLD_TABLE,
    MERCHANTS_SILVER_TABLE,
    MERCHANTS_GOLD_TABLE
)


@dp.table(name=USERS_GOLD_TABLE)
def users_gold():
    return (
        spark.readStream.table(USERS_SILVER_TABLE)
            .filter(F.col('__end_at').isNull())
            .select(
                (
                    F.year('registration_date')*10000 + 
                    F.month('registration_date')*100 + 
                    F.dayofmonth('registration_date')
                ).alias('PK_Tiempo'),
                F.col('user_id'),
                F.col('full_name'),
                F.col('document_id'),
                F.col('email'),
                F.col('phone'),
                F.col('country'),
                F.col('segment'),
                F.col('registration_date'),
                F.when(F.col('country') == 'PE', 'PEN')
                    .when(F.col('country') == 'MX', 'MXN')
                    .when(F.col('country') == 'CO', 'COP')
                    .when(F.col('country') == 'CL', 'CLP')
                    .when(F.col('country') == 'AR', 'ARS')
                    .alias('local_currency')
            )
    )

@dp.table(name=MERCHANTS_GOLD_TABLE)
def merchants_gold():
    return (
        spark.readStream.table(MERCHANTS_SILVER_TABLE)
            .filter(F.col('__end_at').isNull())
            .select(
                (
                    F.year('affiliation_date')*10000 + 
                    F.month('affiliation_date')*100 + 
                    F.dayofmonth('affiliation_date')
                ).alias('PK_Tiempo'),
                F.col('merchant_id'),
                F.col('merchant_name'),
                F.col('category'),
                F.col('country'),
                F.col('affiliation_date'),
                F.col('status'),
                F.col('risk_level'),
                F.when(F.col('country') == 'PE', 'PEN')
                    .when(F.col('country') == 'MX', 'MXN')
                    .when(F.col('country') == 'CO', 'COP')
                    .when(F.col('country') == 'CL', 'CLP')
                    .when(F.col('country') == 'AR', 'ARS')
                    .alias('local_currency')
            )
    )

@dp.table(name=TRANSACTIONS_GOLD_TABLE)
def transactions_gold():
    return (
        spark.readStream.table(TRANSACTIONS_SILVER_TABLE)
            .select(
                (
                    F.year('transaction_date')*10000 + 
                    F.month('transaction_date')*100 + 
                    F.dayofmonth('transaction_date')
                ).alias('PK_Tiempo'),
                F.col('transaction_id'),
                F.col('user_id'),
                F.col('merchant_id'),
                F.col('channel'),
                F.col('transaction_type'),
                F.col('amount'),
                F.col('currency'),
                F.col('transaction_date'),
                F.col('status'),
                F.col('reference_id')
            )
    )
