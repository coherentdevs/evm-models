{{ config(materialized='table') }}

WITH input_and_transaction AS (
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
        SUBSTRING(INPUT, 0, 10) AS METHOD_HEADER
    FROM {{ source('ethereum_managed', 'transactions') }}
),

merged AS (
    SELECT
        *
    FROM input_and_transaction t
    INNER JOIN {{ source('contracts', 'method_fragments') }} m
        ON t.METHOD_HEADER = m.METHOD_ID
        AND t.TO_ADDRESS = m.CONTRACT_ADDRESS
)

SELECT * FROM merged
