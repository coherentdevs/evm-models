{{ config(materialized='table') }}

WITH input_and_transaction AS (
    SELECT
        hex_to_int(BLOCK_NUMBER) as BLOCK_NUMBER,
        BLOCK_HASH,
        hex_to_int(CUMULATIVE_GAS_USED) as CUMULATIVE_GAS_USED,
        hex_to_int(EFFECTIVE_GAS_PRICE) as EFFECTIVE_GAS_PRICE,
        FROM_ADDRESS,
        hex_to_int(GAS) as GAS,
        hex_to_int(GAS_PRICE) as GAS_PRICE,
        hex_to_int(GAS_USED) as GAS_USED,
        LOGS_BLOOM,
        hex_to_int(MAX_FEE_PER_GAS) as MAX_FEE_PER_GAS,
        hex_to_int(MAX_PRIORITY_FEE_PER_GAS) as MAX_PRIORITY_FEE_PER_GAS,
        hex_to_int(NONCE) as NONCE,
        R,
        S,
        hex_to_int(STATUS) as STATUS,
        TO_ADDRESS,
        TRANSACTION_HASH,
        hex_to_int(TRANSACTION_INDEX) as TRANSACTION_INDEX,
        hex_to_int(TYPE) as TYPE,
        V,
        hex_to_int(VALUE) as VALUE,
        ACCESS_LIST,
        INPUT,
        SUBSTRING(INPUT, 0, 10) AS METHOD_HEADER
    FROM {{ source('ethereum_managed', 'transactions') }}
    LIMIT 10000000
),

merged AS (
    SELECT
        t.*,
        m.METHOD_ID,
        m.hashable_signature,
        CASE
            WHEN t.status = '1' THEN m.abi -- only decode successful transactions
            ELSE NULL
        END AS ABI
    FROM input_and_transaction t
    LEFT JOIN {{ source('contracts', 'method_fragments') }} m
        ON t.METHOD_HEADER = m.METHOD_ID
)

SELECT * FROM merged
