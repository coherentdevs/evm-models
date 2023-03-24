import snowflake.snowpark.functions as F



def model(dbt, session):
    dbt.config(materialized="table")
    eth_raw_transactions_df = dbt.source("ethereum_managed", "transactions")
    method_fragments_df = dbt.source("contracts", "method_fragments")

    input_and_transaction_df = eth_raw_transactions_df["TRANSACTION_HASH", "INPUT", "TO_ADDRESS"]
    # replace Input column with Method ID
    method_id_and_transaction_df = input_and_transaction_df.withColumn("Method_ID",
                                                                       F.substring(input_and_transaction_df["INPUT"], 0,
                                                                                   10))
    merged_df = method_id_and_transaction_df.join(method_fragments_df, (method_id_and_transaction_df.col(
        "METHOD_ID") == method_fragments_df.col("METHOD_ID")) & (method_id_and_transaction_df.col(
        "TO_ADDRESS") == method_fragments_df.col("CONTRACT_ADDRESS")), lsuffix="_left", rsuffix="_right", how='inner')
    # clean up joined table
    merged_df = merged_df.with_column_renamed("METHOD_ID_LEFT", "method_id")
    merged_df = merged_df.drop("created_at", "updated_at", "deleted_at", "method_id_right")
    return merged_df
