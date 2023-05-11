{{ config(materialized='incremental', unique_key=['transaction_hash','trace_hash','parent_hash','trace_index'], merge_update_columns = ['hashable_signature', 'decoded_input'], cluster_by=['transaction_hash']) }}

{% if is_incremental() %}
    {% if not var('backfill') %} -- if not backfilling
        WITH traces AS (
            SELECT
                BLOCK_HASH,
                TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) as BLOCK_NUMBER,
                ERROR,
                FROM_ADDRESS,
                TRY_CAST(hex_to_int(GAS) as FLOAT) as GAS,
                TRY_CAST(hex_to_int(GAS_USED) as FLOAT) as GAS_USED,
                TRY_CAST(hex_to_int(TRACE_INDEX) as FLOAT) as TRACE_INDEX,
                INPUT,
                OUTPUT,
                PARENT_HASH,
                REVERT_REASON,
                TO_ADDRESS,
                TRACE_HASH,
                TRANSACTION_HASH,
                TYPE,
                TRY_CAST(hex_to_int(VALUE) as FLOAT) as VALUE,
                SUBSTRING(INPUT, 0, 10) AS METHOD_HEADER
            FROM {{ source(var('optimism_raw_database'), 'traces') }}
            WHERE to_number(SUBSTR(block_number, 3), repeat('X', length(SUBSTR(block_number, 3))))  > (SELECT MAX(block_number) FROM {{ this }}) -- this is the only change
        ),
    {% else %} -- backfilling
        WITH traces AS (
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
                SUBSTRING(INPUT, 0, 10) AS METHOD_HEADER
            FROM {{ source(var('optimism_decoded_database'), 'decoded_optimism_traces') }}
            WHERE DECODED_INPUT is NULL AND INPUT != '0x'
        ),
    {% endif %}
{% else %}
    WITH traces AS (
        SELECT
            BLOCK_HASH,
            TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) as BLOCK_NUMBER,
            ERROR,
            FROM_ADDRESS,
            TRY_CAST(hex_to_int(GAS) as FLOAT) as GAS,
            TRY_CAST(hex_to_int(GAS_USED) as FLOAT) as GAS_USED,
            TRY_CAST(hex_to_int(TRACE_INDEX) as FLOAT) as TRACE_INDEX,
            INPUT,
            OUTPUT,
            PARENT_HASH,
            REVERT_REASON,
            TO_ADDRESS,
            TRACE_HASH,
            TRANSACTION_HASH,
            TYPE,
            TRY_CAST(hex_to_int(VALUE) as FLOAT) as VALUE,
            SUBSTRING(INPUT, 0, 10) AS METHOD_HEADER
        FROM {{ source(var('optimism_raw_database'), 'traces') }}
    ),
{% endif %}

merged AS (
    SELECT
        t.*,
        m.METHOD_ID,
        m.hashable_signature,
        m.abi
    FROM traces t
    LEFT JOIN {{ source(var('contracts_database'), 'method_fragments') }} m
        ON t.METHOD_HEADER = m.METHOD_ID
),

decoded_traces AS (
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
    FROM merged
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
    FROM merged
    WHERE ABI IS NULL
),
{% if not var('backfill') %} -- if not backfilling
    all_traces AS (
        SELECT * FROM decoded_cleaned
        UNION ALL
        SELECT * FROM no_abi
    )
{% else %} -- backfilling
    all_traces AS (
        SELECT * FROM decoded_cleaned WHERE decoded_input IS NOT NULL
    )
{% endif %}


SELECT * FROM all_traces