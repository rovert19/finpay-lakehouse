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

# @dp.table(name=USERS_GOLD_TABLE)
# def users_gold():
#     tranx_agg_per_type = (
#         spark.readStream.table(TRANSACTIONS_SILVER_TABLE)
#             .filter("status == 'aprobado'")
#             .groupBy('user_id', 'transaction_type')
#             .count()
#             .select(
#                 F.col('user_id'),
#                 F.col('transaction_type'),
#                 F.coalesce(F.col("count"), F.lit(0)).alias('count')
#             )
#         )

#     tranx_agg_total = (
#         tranx_agg_per_type
#             .groupBy('user_id')
#             .agg(F.sum('count').alias('total_tranx'))
#         )

#     tranx_reversed_rate = (
#         tranx_agg_per_type
#             .filter(F.col('transaction_type') == 'reversa').alias('r')
#             .join(tranx_agg_total, on='user_id', how='left')
#             .select(
#                 F.col('r.user_id'),
#                 F.when(F.col('total_tranx') > 0, F.col('r.count') / F.col('total_tranx') * 100)
#                     .otherwise(F.lit(0))
#                     .cast('int')
#                     .alias('reversal_rate')
#             )
#             .withColumn('risk_level',
#                 F.when(F.col('reversal_rate') >= 50, 3)
#                 .when(F.col('reversal_rate') >= 25, 2)
#                 .when(F.col('reversal_rate') >= 10, 1)
#                 .otherwise(F.lit(0))
#             )
#     )

#     return (
#         spark.readStream.table(USERS_SILVER_TABLE)
#             .filter(F.col('__end_at').isNull()).alias('u')
#             .join(tranx_reversed_rate, on='user_id', how='left')
#             .select(
#                 F.col('u.user_id'),
#                 F.col('u.full_name'),
#                 F.col('u.document_id'),
#                 F.col('u.email'),
#                 F.col('u.phone'),
#                 F.col('u.country'),
#                 F.col('u.segment'),
#                 F.col('u.registration_date'),
#                 F.col('reversal_rate'),
#                 F.col('risk_level').alias('user_risk_level')
#             )
#     )


# @dp.table(name=MERCHANTS_GOLD_TABLE)
# def merchants_gold():
#     tranx_agg_per_type = (
#         spark.readStream.table(TRANSACTIONS_SILVER_TABLE)
#             .filter("status == 'aprobado'")
#             .groupBy('merchant_id', 'transaction_type')
#             .count()
#             .select(
#                 F.col('merchant_id'),
#                 F.col('transaction_type'),
#                 F.coalesce(F.col("count"), F.lit(0)).alias('count')
#             )
#         )

#     tranx_agg_total = (
#         tranx_agg_per_type
#             .groupBy('merchant_id')
#             .agg(F.sum('count').alias('total_tranx'))
#         )

#     tranx_reversed_rate = (
#         tranx_agg_per_type
#             .filter(F.col('transaction_type') == 'reversa').alias('r')
#             .join(tranx_agg_total, on='merchant_id', how='left')
#             .select(
#                 F.col('r.merchant_id').alias('merchant_id'),
#                 F.when(F.col('total_tranx') > 0, F.col('r.count') / F.col('total_tranx') * 100)
#                     .otherwise(F.lit(0))
#                     .cast('int')
#                     .alias('reversal_rate')
#             )
#     )

#     return (
#         spark.readStream.table(MERCHANTS_SILVER_TABLE)
#             .filter(F.col('__end_at').isNull()).alias('m')
#             .join(tranx_reversed_rate, on='merchant_id', how='left')
#             .select(
#                 F.col('m.merchant_id'),
#                 F.col('m.merchant_name'),
#                 F.col('m.category'),
#                 F.col('m.country'),
#                 F.col('m.affiliation_date'),
#                 F.col('m.status'),
#                 F.col('m.risk_level'),
#                 F.col('reversal_rate')
#             )
#     )


# @dp.table(name=TRANSACTIONS_GOLD_TABLE)
# def transactions_gold():
#     users = (
#         spark.readStream.table(USERS_SILVER_TABLE)
#             .filter(F.col('__end_at').isNull())
#             .select(
#                 F.col('user_id'),
#                 F.col('country').alias('user_country'),
#                 F.col('segment'),
#                 F.when(F.col('country') == 'PE', 'PEN')
#                     .when(F.col('country') == 'MX', 'MXN')
#                     .when(F.col('country') == 'CO', 'COP')
#                     .when(F.col('country') == 'CL', 'CLP')
#                     .when(F.col('country') == 'AR', 'ARS')
#                     .alias('user_currency')
#             )
#     )

#     merchants = (
#         spark.readStream.table(MERCHANTS_SILVER_TABLE)
#             .filter(F.col('__end_at').isNull())
#             .select(
#                 F.col('merchant_id'),
#                 F.col('country').alias('merchant_country'),
#                 F.col('status').alias('merchant_status'),
#                 F.col('risk_level'),
#                 F.when(F.col('country') == 'PE', 'PEN')
#                     .when(F.col('country') == 'MX', 'MXN')
#                     .when(F.col('country') == 'CO', 'COP')
#                     .when(F.col('country') == 'CL', 'CLP')
#                     .when(F.col('country') == 'AR', 'ARS')
#                     .alias('merchant_currency')
#             )
#     )

#     transactions = spark.readStream.table(TRANSACTIONS_SILVER_TABLE)
  
#     return (
#         transactions.alias('t')
#             .join(users.alias('u'), on='user_id', how= 'left')
#             .join(merchants.alias('m'), on='merchant_id', how= 'left')
#             .select(
#                 (
#                     F.year('transaction_date')*10000 + 
#                     F.month('transaction_date')*100 + 
#                     F.dayofmonth('transaction_date')
#                 ).alias('PK_Tiempo'),
#                 F.col('transaction_id'),
#                 F.col('t.user_id'),
#                 F.col('t.merchant_id'),
#                 F.col('channel'),
#                 F.col('transaction_type'),
#                 F.col('amount'),
#                 F.col('currency'),
#                 F.col('transaction_date'),
#                 F.col('status'),
#                 F.col('reference_id'),
#                 F.when((F.col('merchant_currency') != F.col('currency')) | (F.col('user_currency') != F.col('currency')), 1)
#                     .otherwise(0)
#                     .alias('currency_mismatch'),
#                 F.when((F.col('merchant_currency') != F.col('currency')) & 
#                        (F.col('currency') != 'USD') &
#                        ((F.col('risk_level') == 'alto') | F.col('risk_level').isNull()) & 
#                        (F.col('merchant_status') != 'activo'), 100)
#                     .when((F.col('merchant_currency') != F.col('currency')) & 
#                           (F.col('currency') != 'USD') & 
#                           ((F.col('risk_level') == 'alto') | F.col('risk_level').isNull()), 80)
#                     .when((F.col('merchant_currency') != F.col('currency')) & 
#                           (F.col('currency') != 'USD') & 
#                           (F.col('risk_level') == 'medio'), 60)
#                     .when((F.col('merchant_currency') != F.col('currency')) & 
#                           (F.col('currency') != 'USD') & 
#                           (F.col('risk_level') == 'bajo'), 50)
#                     .when(((F.col('merchant_currency') == F.col('currency')) | (F.col('currency') == 'USD')) & 
#                           ((F.col('risk_level') == 'alto') | F.col('risk_level').isNull()), 30)
#                     .when(((F.col('merchant_currency') == F.col('currency')) | (F.col('currency') == 'USD')) & 
#                           (F.col('risk_level') == 'medio'), 10)
#                     .otherwise(0)
#                     .alias('risk_score')
#             )
#     )