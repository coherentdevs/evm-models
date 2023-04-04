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
    FROM {{ source('ethereum_managed', 'logs') }}
    LIMIT 10000000
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

distinct_event_fragments_table AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_id) as row_num
    FROM {{ source('contracts', 'event_fragments') }}
),

merged AS (
    SELECT
        l.*,
        d.event_id as matched_event_id,
        d.full_signature,
        d.abi,
        d.hashable_signature
    FROM logs_with_event_id l
    LEFT JOIN distinct_event_fragments_table d
        ON l.extracted_event_id = d.event_id AND d.row_num = 1
)

SELECT * FROM merged