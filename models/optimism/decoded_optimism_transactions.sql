{{ config(materialized='incremental', unique_key='transaction_hash', merge_update_columns = ['hashable_signature', 'decoded_input'], cluster_by=['from_address']) }}

{% if is_incremental() %}
    {% if not var('backfill') %} -- if not backfilling
        WITH input_and_transaction_optimism AS (
            SELECT
                TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) as BLOCK_NUMBER,
                BLOCK_HASH,
                TRY_CAST(hex_to_int(CUMULATIVE_GAS_USED) as FLOAT) as CUMULATIVE_GAS_USED,
                FROM_ADDRESS,
                TRY_CAST(hex_to_int(GAS) as FLOAT) as GAS,
                TRY_CAST(hex_to_int(GAS_PRICE) as FLOAT) as GAS_PRICE,
                TRY_CAST(hex_to_int(GAS_USED) as FLOAT) as GAS_USED,
                INPUT,
                LOGS_BLOOM,
                TRY_CAST(hex_to_int(NONCE) as NUMBER) as NONCE,
                R,
                S,
                True as Status,
                TO_ADDRESS,
                TRANSACTION_HASH,
                TRY_CAST(hex_to_int(TRANSACTION_INDEX) as NUMBER) as TRANSACTION_INDEX,
                V,
                TRY_CAST(hex_to_int(VALUE) as FLOAT) as VALUE,
                QUEUE_ORIGIN,
                L1_TX_ORIGIN,
                TRY_CAST(hex_to_int(L1_BLOCK_NUMBER) as FLOAT) as L1_BLOCK_NUMBER,
                TRY_CAST(hex_to_int(L1_TIMESTAMP) as TIMESTAMP) as L1_TIMESTAMP,
                TRY_CAST(hex_to_int(INDEX) as NUMBER) as INDEX,
                TRY_CAST(hex_to_int(QUEUE_INDEX) as NUMBER) as QUEUE_INDEX,
                RAW_TRANSACTION,
                TRY_CAST(hex_to_int(L1_FEE) as FLOAT) as L1_FEE,
                TRY_CAST(L1_FEE_SCALAR as FLOAT) as L1_FEE_SCALAR,
                TRY_CAST(hex_to_int(L1_GAS_PRICE) as FLOAT) as L1_GAS_PRICE,
                TRY_CAST(hex_to_int(L1_GAS_USED) as FLOAT) as L1_GAS_USED,
                SUBSTRING(INPUT, 0, 10) AS METHOD_HEADER
            FROM {{ source(var('optimism_raw_database'), 'transactions') }}
            WHERE STATUS = '0x1' AND to_number(SUBSTR(block_number, 3), repeat('X', length(SUBSTR(block_number, 3))))  > (SELECT MAX(block_number) FROM {{ this }}) -- this is the only change
        ),
    {% else %} -- backfilling
        WITH input_and_transaction_optimism AS (
            SELECT
                BLOCK_NUMBER,
                BLOCK_HASH,
                CUMULATIVE_GAS_USED,
                FROM_ADDRESS,
                GAS,
                GAS_PRICE,
                GAS_USED,
                INPUT,
                LOGS_BLOOM,
                NONCE,
                R,
                S,
                Status,
                TO_ADDRESS,
                TRANSACTION_HASH,
                TRANSACTION_INDEX,
                V,
                VALUE,
                QUEUE_ORIGIN,
                L1_TX_ORIGIN,
                L1_BLOCK_NUMBER,
                L1_TIMESTAMP,
                INDEX,
                QUEUE_INDEX,
                RAW_TRANSACTION,
                L1_FEE,
                L1_FEE_SCALAR,
                L1_GAS_PRICE,
                L1_GAS_USED
            FROM {{ source(var('optimism_decoded_database'), 'decoded_optimism_transactions') }}
            WHERE DECODED_INPUT is NULL AND INPUT != '0x'
        ),
    {% endif %}
{% else %} -- full refresh
    WITH input_and_transaction_optimism AS (
        SELECT
            TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) as BLOCK_NUMBER,
            BLOCK_HASH,
            TRY_CAST(hex_to_int(CUMULATIVE_GAS_USED) as FLOAT) as CUMULATIVE_GAS_USED,
            FROM_ADDRESS,
            TRY_CAST(hex_to_int(GAS) as FLOAT) as GAS,
            TRY_CAST(hex_to_int(GAS_PRICE) as FLOAT) as GAS_PRICE,
            TRY_CAST(hex_to_int(GAS_USED) as FLOAT) as GAS_USED,
            INPUT,
            LOGS_BLOOM,
            TRY_CAST(hex_to_int(NONCE) as NUMBER) as NONCE,
            R,
            S,
            True as Status,
            TO_ADDRESS,
            TRANSACTION_HASH,
            TRY_CAST(hex_to_int(TRANSACTION_INDEX) as NUMBER) as TRANSACTION_INDEX,
            V,
            TRY_CAST(hex_to_int(VALUE) as FLOAT) as VALUE,
            QUEUE_ORIGIN,
            L1_TX_ORIGIN,
            TRY_CAST(hex_to_int(L1_BLOCK_NUMBER) as FLOAT) as L1_BLOCK_NUMBER,
            TRY_CAST(hex_to_int(L1_TIMESTAMP) as TIMESTAMP) as L1_TIMESTAMP,
            TRY_CAST(hex_to_int(INDEX) as NUMBER) as INDEX,
            TRY_CAST(hex_to_int(QUEUE_INDEX) as NUMBER) as QUEUE_INDEX,
            RAW_TRANSACTION,
            TRY_CAST(hex_to_int(L1_FEE) as FLOAT) as L1_FEE,
            TRY_CAST(L1_FEE_SCALAR as FLOAT) as L1_FEE_SCALAR,
            TRY_CAST(hex_to_int(L1_GAS_PRICE) as FLOAT) as L1_GAS_PRICE,
            TRY_CAST(hex_to_int(L1_GAS_USED) as FLOAT) as L1_GAS_USED,
            SUBSTRING(INPUT, 0, 10) AS METHOD_HEADER
        FROM {{ source(var('optimism_raw_database'), 'transactions') }}
        WHERE STATUS = '0x1' -- only interested in successful transactions
    ),
{% endif %}

merged AS (
    SELECT
        t.*,
        m.METHOD_ID,
        m.hashable_signature,
        m.abi
    FROM input_and_transaction_optimism t
    LEFT JOIN {{ source(var('contracts_database'), 'method_fragments') }} m
        ON t.METHOD_HEADER = m.METHOD_ID
),

decoded_transactions AS (
    SELECT
        BLOCK_NUMBER,
        BLOCK_HASH,
        CUMULATIVE_GAS_USED,
        FROM_ADDRESS,
        GAS,
        GAS_PRICE,
        GAS_USED,
        INPUT,
        LOGS_BLOOM,
        NONCE,
        R,
        S,
        Status,
        TO_ADDRESS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        V,
        VALUE,
        QUEUE_ORIGIN,
        L1_TX_ORIGIN,
        L1_BLOCK_NUMBER,
        L1_TIMESTAMP,
        INDEX,
        QUEUE_INDEX,
        RAW_TRANSACTION,
        L1_FEE,
        L1_FEE_SCALAR,
        L1_GAS_PRICE,
        L1_GAS_USED,
        HASHABLE_SIGNATURE,
        decode_input(abi, SUBSTRING(INPUT,11))[0] as decoded_result,
        decode_input(abi, SUBSTRING(INPUT,11))[1] as decoded_success
    FROM merged
    WHERE ABI IS NOT NULL
),

decoded_cleaned AS (
    SELECT
        BLOCK_NUMBER,
        BLOCK_HASH,
        CUMULATIVE_GAS_USED,
        FROM_ADDRESS,
        GAS,
        GAS_PRICE,
        GAS_USED,
        INPUT,
        LOGS_BLOOM,
        NONCE,
        R,
        S,
        Status,
        TO_ADDRESS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        V,
        VALUE,
        QUEUE_ORIGIN,
        L1_TX_ORIGIN,
        L1_BLOCK_NUMBER,
        L1_TIMESTAMP,
        INDEX,
        QUEUE_INDEX,
        RAW_TRANSACTION,
        L1_FEE,
        L1_FEE_SCALAR,
        L1_GAS_PRICE,
        L1_GAS_USED,
        HASHABLE_SIGNATURE,
        CASE
            WHEN decoded_success = True THEN decoded_result
            ELSE NULL
        END AS DECODED_INPUT
    FROM decoded_transactions
),

no_abi AS (
    SELECT
        BLOCK_NUMBER,
        BLOCK_HASH,
        CUMULATIVE_GAS_USED,
        FROM_ADDRESS,
        GAS,
        GAS_PRICE,
        GAS_USED,
        INPUT,
        LOGS_BLOOM,
        NONCE,
        R,
        S,
        Status,
        TO_ADDRESS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        V,
        VALUE,
        QUEUE_ORIGIN,
        L1_TX_ORIGIN,
        L1_BLOCK_NUMBER,
        L1_TIMESTAMP,
        INDEX,
        QUEUE_INDEX,
        RAW_TRANSACTION,
        L1_FEE,
        L1_FEE_SCALAR,
        L1_GAS_PRICE,
        L1_GAS_USED,
        NULL AS HASHABLE_SIGNATURE,
        NULL AS DECODED_INPUT
    FROM merged
    WHERE ABI IS NULL
),

{% if not var('backfill') %} -- if not backfilling
    all_transactions AS (
        SELECT * FROM decoded_cleaned
        UNION ALL
        SELECT * FROM no_abi
    )
{% else %} -- backfilling
    all_transactions AS (
        SELECT * FROM decoded_cleaned WHERE DECODED_INPUT IS NOT NULL
    )
{% endif %}

SELECT * FROM all_transactions