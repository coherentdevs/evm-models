{{ config(materialized='table') }}

WITH decoded_traces AS (
    SELECT
        BLOCK_HASH,
        BLOCK_NUMBER,
        ERROR,
        FROM_ADDRESS,
        GAS,
        GAS_USED,
        TRACE_INDEX,
        INPUT,
        OUTPUT,
        PARENT_HASH,
        REVERT_REASON,
        TO_ADDRESS,
        TRACE_HASH,
        TRANSACTION_HASH,
        TYPE,
        VALUE,
        ABI,
        HASHABLE_SIGNATURE,
        decode_input(abi, SUBSTRING(INPUT,11))[0] as decoded_result,
        decode_input(abi, SUBSTRING(INPUT,11))[1] as decoded_success
    FROM {{ ref('raw_traces_with_method_fragments_table') }}
    WHERE ABI IS NOT NULL
),

decoded_cleaned AS (
    SELECT
        BLOCK_HASH,
        BLOCK_NUMBER,
        ERROR,
        FROM_ADDRESS,
        GAS,
        GAS_USED,
        TRACE_INDEX,
        INPUT,
        OUTPUT,
        PARENT_HASH,
        REVERT_REASON,
        TO_ADDRESS,
        TRACE_HASH,
        TRANSACTION_HASH,
        TYPE,
        VALUE,
        HASHABLE_SIGNATURE,
        CASE
            WHEN decoded_success = True THEN decoded_result
            ELSE NULL
        END AS DECODED_INPUT
    FROM decoded_traces
),

no_abi AS (
    SELECT
        BLOCK_HASH,
        BLOCK_NUMBER,
        ERROR,
        FROM_ADDRESS,
        GAS,
        GAS_USED,
        TRACE_INDEX,
        INPUT,
        OUTPUT,
        PARENT_HASH,
        REVERT_REASON,
        TO_ADDRESS,
        TRACE_HASH,
        TRANSACTION_HASH,
        TYPE,
        VALUE,
        NULL AS HASHABLE_SIGNATURE,
        NULL AS DECODED_INPUT
    FROM {{ ref('raw_traces_with_method_fragments_table') }}
    WHERE ABI IS NULL
),

all_traces AS (
    SELECT * FROM decoded_cleaned
    UNION ALL
    SELECT * FROM no_abi
)

SELECT * FROM all_traces