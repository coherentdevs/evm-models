{{ config(materialized='table') }}

WITH input_and_transaction AS (
    SELECT
        BLOCK_NUMBER,
        TRANSACTION_HASH,
        INPUT,
        TO_ADDRESS,
        SUBSTRING(INPUT, 0, 10) AS Method_ID
    FROM {{ source('ethereum_managed', 'transactions') }}
),

merged AS (
    SELECT
        hex_to_int(t.BLOCK_NUMBER) as BLOCK_NUMBER,
        t.TRANSACTION_HASH,
        t.INPUT,
        m.HASHABLE_SIGNATURE
    FROM input_and_transaction t
    INNER JOIN {{ source('contracts', 'method_fragments') }} m
        ON t.Method_ID = m.METHOD_ID
        AND t.TO_ADDRESS = m.CONTRACT_ADDRESS
)

SELECT * FROM merged LIMIT 10000000
