{{ config(materialized='table') }}

WITH logs AS (
    SELECT
        ADDRESS,
        BLOCK_HASH,
        hex_to_int(BLOCK_NUMBER) as BLOCK_NUMBER,
        DATA,
        hex_to_int(LOG_INDEX) as LOG_INDEX,
        TOPICS,
        TRANSACTION_HASH,
        hex_to_int(TRANSACTION_INDEX) as TRANSACTION_INDEX,
        REMOVED
    FROM {{ source('ethereum_raw_data', 'logs') }}
),

logs_with_event_id AS (
    SELECT
        ADDRESS,
        BLOCK_HASH,
        BLOCK_NUMBER,
        DATA,
        LOG_INDEX,
        TOPICS,
        REPLACE(REPLACE(REPLACE(SPLIT(TOPICS, ',')[0], '[', ''), ']', ''), '\"', '') as extracted_event_id, -- Extract the first element of the TOPICS array as event_id
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        REMOVED
    FROM logs
),

merged AS (
    SELECT
        l.*,
        e.event_id,
        e.abi,
        e.hashable_signature
    FROM logs_with_event_id l
    LEFT JOIN {{ source('evm_contract_fragments_data', 'event_fragments') }} e
        ON l.extracted_event_id = e.event_id
)

SELECT * FROM merged