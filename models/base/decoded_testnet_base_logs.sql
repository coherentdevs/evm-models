{{ config(materialized='incremental', unique_key=['transaction_hash','log_index'], merge_update_columns = ['hashable_signature', 'decoded_result'], cluster_by=['transaction_hash']) }}

{% if is_incremental() %}
    {% if not var('backfill') %} -- if not backfilling
        WITH logs AS (
            SELECT
                ADDRESS,
                BLOCK_HASH,
                TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) as BLOCK_NUMBER,
                DATA,
                TRY_CAST(hex_to_int(LOG_INDEX) as FLOAT) as LOG_INDEX,
                TOPICS,
                TRANSACTION_HASH,
                TRY_CAST(hex_to_int(TRANSACTION_INDEX) as FLOAT) as TRANSACTION_INDEX,
                REMOVED
            FROM {{ source(var('testnet_base_raw_database'), 'logs') }}
            WHERE to_number(SUBSTR(block_number, 3), repeat('X', length(SUBSTR(block_number, 3))))  > (SELECT MAX(block_number) FROM {{ this }}) -- this is the only change
        ),
    {% else %} -- backfilling
        WITH logs AS (
            SELECT
                ADDRESS,
                BLOCK_HASH,
                BLOCK_NUMBER,
                DATA,
                LOG_INDEX,
                TOPICS,
                TRANSACTION_HASH,
                TRANSACTION_INDEX,
                REMOVED
            FROM {{ source(var('testnet_base_decoded_database'), 'decoded_testnet_base_logs') }}
            WHERE DECODED_RESULT is NULL
        ),
    {% endif %}
{% else %}
    WITH logs AS (
        SELECT
            ADDRESS,
            BLOCK_HASH,
            TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) as BLOCK_NUMBER,
            DATA,
            TRY_CAST(hex_to_int(LOG_INDEX) as FLOAT) as LOG_INDEX,
            TOPICS,
            TRANSACTION_HASH,
            TRY_CAST(hex_to_int(TRANSACTION_INDEX) as FLOAT) as TRANSACTION_INDEX,
            REMOVED
        FROM {{ source(var('testnet_base_raw_database'), 'logs') }}
    ),
{% endif %}

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
    LEFT JOIN {{ source(var('contracts_database'), 'event_fragments') }} e
        ON l.extracted_event_id = e.event_id
),

decoded_logs AS (
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
    FROM merged
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
    FROM merged
    WHERE ABI IS NULL
),
{% if not var('backfill') %} -- if not backfilling
    full_table AS
    (
      SELECT * FROM no_abi
      UNION ALL
      SELECT * FROM decoded_cleaned
    )
{% else %} -- backfilling
    full_table AS
    (
      SELECT * FROM decoded_cleaned WHERE DECODED_RESULT IS NOT NULL
    )
{% endif %}

select * from full_table