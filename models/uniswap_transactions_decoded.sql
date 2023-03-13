{{ config(materialized='view') }}

SELECT
    BLOCK_HASH,
    hex_to_int(BLOCK_NUMBER) as BLOCK_NUMBER,
    CHAIN_ID,
    hex_to_int(CUMULATIVE_GAS_USED) as CUMULATIVE_GAS_USED,
    hex_to_int(EFFECTIVE_GAS_PRICE) as EFFECTIVE_GAS_PRICE,
    FROM_ADDRESS,
    hex_to_int(GAS) as GAS,
    hex_to_int(GAS_USED) as GAS_USED,
    INPUT as INPUT,
    LOGS_BLOOM,
    NONCE,
    R,
    S,
    hex_to_int(STATUS) as STATUS,
    TO_ADDRESS,
    TRANSACTION_HASH,
    hex_to_int(TRANSACTION_INDEX) as TRANSACTION_INDEX,
    TYPE,
    V,
    hex_to_int(VALUE) as VALUE
FROM PC_DBT_DB.DBT_.UNISWAP_TRANSACTIONS
    LIMIT 10