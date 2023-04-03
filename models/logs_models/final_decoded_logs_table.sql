{{ config(materialized='table') }}
WITH decoded AS (
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
        CASE
            WHEN decoded_success = True THEN decoded_result
            ELSE NULL
        END AS DECODED_RESULT
    FROM {{ ref('decode_logs') }}
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
        NULL AS FULL_SIGNATURE,
        NULL AS DECODED_RESULT
    FROM {{ ref('raw_logs_with_event_fragments_table') }}
    WHERE ABI IS NULL
),

full_table AS
(
  SELECT * FROM no_abi
  UNION ALL
  SELECT * FROM decoded
)

SELECT * FROM full_table