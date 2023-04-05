{{ config(materialized='table') }}

WITH traces AS (
    SELECT
        BLOCK_HASH,
        hex_to_int(BLOCK_NUMBER) as BLOCK_NUMBER,
        ERROR,
        FROM_ADDRESS,
        hex_to_int(GAS) as GAS,
        hex_to_int(GAS_USED) as GAS_USED,
        TRACE_INDEX,
        INPUT,
        OUTPUT,
        PARENT_HASH,
        REVERT_REASON,
        TO_ADDRESS,
        TRACE_HASH,
        TRANSACTION_HASH,
        TYPE,
        hex_to_int(VALUE) as VALUE,
        SUBSTRING(INPUT, 0, 10) AS METHOD_HEADER
    FROM {{ source('ethereum_managed', 'traces') }}
),

merged AS (
    SELECT
        t.*,
        m.METHOD_ID,
        m.hashable_signature,
        m.abi
    FROM traces t
    LEFT JOIN {{ source('contracts', 'method_fragments') }} m
        ON t.METHOD_HEADER = m.METHOD_ID
)

SELECT * FROM merged
