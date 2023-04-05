{{ config(materialized='table') }}

WITH decoded_logs AS (
    SELECT
        ADDRESS,
        BLOCK_HASH,
        BLOCK_NUMBER,
        DATA,
        LOG_INDEX,
        TOPICS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        REMOVED,
        ABI,
        HASHABLE_SIGNATURE,
        decode_logs(abi, SUBSTRING(data,3), topics)[0] as decoded_result,
        decode_logs(abi, SUBSTRING(data,3), topics)[1] as decoded_success
    FROM {{ ref('raw_logs_with_event_fragments_table') }}
    WHERE ABI IS NOT NULL
),

decoded_cleaned AS (
    SELECT
        ADDRESS,
        BLOCK_HASH,
        BLOCK_NUMBER,
        DATA,
        LOG_INDEX,
        TOPICS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        REMOVED,
        HASHABLE_SIGNATURE,
        CASE
            WHEN decoded_success = True THEN decoded_result
            ELSE NULL
        END AS DECODED_RESULT
    FROM decoded_logs
),

no_abi AS (
    SELECT
        ADDRESS,
        BLOCK_HASH,
        BLOCK_NUMBER,
        DATA,
        LOG_INDEX,
        TOPICS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        REMOVED,
        NULL AS HASHABLE_SIGNATURE,
        NULL AS DECODED_RESULT
    FROM {{ ref('raw_logs_with_event_fragments_table') }}
    WHERE ABI IS NULL
),

full_table AS
(
  SELECT * FROM no_abi
  UNION ALL
  SELECT * FROM decoded_cleaned
)

select * from full_table