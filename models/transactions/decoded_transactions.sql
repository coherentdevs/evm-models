{{ config(materialized='table') }}

WITH decoded_transactions AS (
    SELECT
        BLOCK_NUMBER,
        BLOCK_HASH,
        CUMULATIVE_GAS_USED,
        EFFECTIVE_GAS_PRICE,
        FROM_ADDRESS,
        GAS,
        GAS_PRICE,
        GAS_USED,
        LOGS_BLOOM,
        MAX_FEE_PER_GAS,
        MAX_PRIORITY_FEE_PER_GAS,
        NONCE,
        R,
        S,
        STATUS,
        TO_ADDRESS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        TYPE,
        V,
        VALUE,
        ACCESS_LIST,
        INPUT,
        HASHABLE_SIGNATURE,
        decode_input(abi, SUBSTRING(INPUT,11))[0] as decoded_result,
        decode_input(abi, SUBSTRING(INPUT,11))[1] as decoded_success
    FROM {{ ref('raw_transactions_with_method_fragments_table') }}
    WHERE ABI IS NOT NULL
),

decoded_cleaned AS (
    SELECT
        BLOCK_NUMBER,
        BLOCK_HASH,
        CUMULATIVE_GAS_USED,
        EFFECTIVE_GAS_PRICE,
        FROM_ADDRESS,
        GAS,
        GAS_PRICE,
        GAS_USED,
        LOGS_BLOOM,
        MAX_FEE_PER_GAS,
        MAX_PRIORITY_FEE_PER_GAS,
        NONCE,
        R,
        S,
        STATUS,
        TO_ADDRESS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        TYPE,
        V,
        VALUE,
        ACCESS_LIST,
        INPUT,
        HASHABLE_SIGNATURE,
        CASE
            WHEN decoded_success = True THEN decoded_result
            ELSE NULL
        END AS DECODED_INPUT
    FROM decoded_transactions
),

no_abi AS (
    SELECT
        BLOCK_NUMBER,
        BLOCK_HASH,
        CUMULATIVE_GAS_USED,
        EFFECTIVE_GAS_PRICE,
        FROM_ADDRESS,
        GAS,
        GAS_PRICE,
        GAS_USED,
        LOGS_BLOOM,
        MAX_FEE_PER_GAS,
        MAX_PRIORITY_FEE_PER_GAS,
        NONCE,
        R,
        S,
        STATUS,
        TO_ADDRESS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        TYPE,
        V,
        VALUE,
        ACCESS_LIST,
        INPUT,
        NULL AS HASHABLE_SIGNATURE,
        NULL AS DECODED_INPUT
    FROM {{ ref('raw_transactions_with_method_fragments_table') }}
    WHERE ABI IS NULL
),

all_transactions AS (
    SELECT * FROM decoded_cleaned
    UNION ALL
    SELECT * FROM no_abi
)

SELECT * FROM all_transactions