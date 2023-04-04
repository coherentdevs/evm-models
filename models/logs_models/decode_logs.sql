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
)

select * from decoded_logs_parsed