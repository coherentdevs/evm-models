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
        FULL_SIGNATURE,
        ABI,
        HASHABLE_SIGNATURE,
        decode_logs(abi, SUBSTRING(data,3), topics) as decoded
    FROM {{ ref('raw_logs_with_event_fragments_table') }}
    WHERE ABI IS NOT NULL
),

decoded_logs_parsed AS (
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
        FULL_SIGNATURE,
        ABI,
        HASHABLE_SIGNATURE,
        decoded[0] as decoded_result,
        decoded[1] as decoded_success
    FROM decoded_logs
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
    FROM decoded_logs_parsed
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