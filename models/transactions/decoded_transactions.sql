{{ config(materialized='incremental', unique_key='transaction_hash', cluster_by=['from_address']) }}

{% if is_incremental() %}
    WITH input_and_transaction AS (
        SELECT
            TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) as BLOCK_NUMBER,
            BLOCK_HASH,
            TRY_CAST(hex_to_int(CUMULATIVE_GAS_USED) as FLOAT) as CUMULATIVE_GAS_USED,
            TRY_CAST(hex_to_int(EFFECTIVE_GAS_PRICE) as FLOAT) as EFFECTIVE_GAS_PRICE,
            FROM_ADDRESS,
            TRY_CAST(hex_to_int(GAS) as FLOAT) as GAS,
            TRY_CAST(hex_to_int(GAS_PRICE) as FLOAT) as GAS_PRICE,
            TRY_CAST(hex_to_int(GAS_USED) as FLOAT) as GAS_USED,
            LOGS_BLOOM,
            TRY_CAST(hex_to_int(MAX_FEE_PER_GAS) as FLOAT) as MAX_FEE_PER_GAS,
            TRY_CAST(hex_to_int(MAX_PRIORITY_FEE_PER_GAS) as FLOAT) as MAX_PRIORITY_FEE_PER_GAS,
            TRY_CAST(hex_to_int(NONCE) as NUMBER) as NONCE,
            R,
            S,
            hex_to_int(STATUS) as STATUS,
            TO_ADDRESS,
            TRANSACTION_HASH,
            hex_to_int(TRANSACTION_INDEX) as TRANSACTION_INDEX,
            TRY_CAST(hex_to_int(TYPE) as FLOAT) as TYPE,
            V,
            TRY_CAST(hex_to_int(VALUE) as FLOAT) as VALUE,
            ACCESS_LIST,
            INPUT,
            SUBSTRING(INPUT, 0, 10) AS METHOD_HEADER
        FROM {{ source('ethereum_managed', 'transactions') }}
        WHERE to_number(SUBSTR(block_number, 3), repeat('X', length(SUBSTR(block_number, 3))))  > (SELECT MAX(CAST(block_number AS INTEGER)) FROM {{ this }}) -- this is the only change
    ),
{% else %}
    WITH input_and_transaction AS (
        SELECT
            TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) as BLOCK_NUMBER,
            BLOCK_HASH,
            TRY_CAST(hex_to_int(CUMULATIVE_GAS_USED) as FLOAT) as CUMULATIVE_GAS_USED,
            TRY_CAST(hex_to_int(EFFECTIVE_GAS_PRICE) as FLOAT) as EFFECTIVE_GAS_PRICE,
            FROM_ADDRESS,
            TRY_CAST(hex_to_int(GAS) as FLOAT) as GAS,
            TRY_CAST(hex_to_int(GAS_PRICE) as FLOAT) as GAS_PRICE,
            TRY_CAST(hex_to_int(GAS_USED) as FLOAT) as GAS_USED,
            LOGS_BLOOM,
            TRY_CAST(hex_to_int(MAX_FEE_PER_GAS) as FLOAT) as MAX_FEE_PER_GAS,
            TRY_CAST(hex_to_int(MAX_PRIORITY_FEE_PER_GAS) as FLOAT) as MAX_PRIORITY_FEE_PER_GAS,
            TRY_CAST(hex_to_int(NONCE) as NUMBER) as NONCE,
            R,
            S,
            hex_to_int(STATUS) as STATUS,
            TO_ADDRESS,
            TRANSACTION_HASH,
            hex_to_int(TRANSACTION_INDEX) as TRANSACTION_INDEX,
            TRY_CAST(hex_to_int(TYPE) as FLOAT) as TYPE,
            V,
            TRY_CAST(hex_to_int(VALUE) as FLOAT) as VALUE,
            ACCESS_LIST,
            INPUT,
            SUBSTRING(INPUT, 0, 10) AS METHOD_HEADER
        FROM {{ source('ethereum_managed', 'transactions') }}
        LIMIT 1000000
    ),
{% endif %}

merged AS (
    SELECT
        t.*,
        m.METHOD_ID,
        m.hashable_signature,
        CASE
            WHEN t.status = '1' THEN m.abi -- only decode successful transactions
            ELSE NULL
        END AS ABI
    FROM input_and_transaction t
    LEFT JOIN {{ source('contracts', 'method_fragments') }} m
        ON t.METHOD_HEADER = m.METHOD_ID
),

decoded_transactions AS (
    SELECT
        BLOCK_NUMBER,
        BLOCK_HASH,
        CUMULATIVE_GAS_USED,
        EFFECTIVE_GAS_PRICE,
        FROM_ADDRESS,
        GAS,
        GAS_PRICE,
        GAS_USED,
        LOGS_BLOOM,
        MAX_FEE_PER_GAS,
        MAX_PRIORITY_FEE_PER_GAS,
        NONCE,
        R,
        S,
        STATUS,
        TO_ADDRESS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        TYPE,
        V,
        VALUE,
        ACCESS_LIST,
        INPUT,
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
        EFFECTIVE_GAS_PRICE,
        FROM_ADDRESS,
        GAS,
        GAS_PRICE,
        GAS_USED,
        LOGS_BLOOM,
        MAX_FEE_PER_GAS,
        MAX_PRIORITY_FEE_PER_GAS,
        NONCE,
        R,
        S,
        STATUS,
        TO_ADDRESS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        TYPE,
        V,
        VALUE,
        ACCESS_LIST,
        INPUT,
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
        EFFECTIVE_GAS_PRICE,
        FROM_ADDRESS,
        GAS,
        GAS_PRICE,
        GAS_USED,
        LOGS_BLOOM,
        MAX_FEE_PER_GAS,
        MAX_PRIORITY_FEE_PER_GAS,
        NONCE,
        R,
        S,
        STATUS,
        TO_ADDRESS,
        TRANSACTION_HASH,
        TRANSACTION_INDEX,
        TYPE,
        V,
        VALUE,
        ACCESS_LIST,
        INPUT,
        NULL AS HASHABLE_SIGNATURE,
        NULL AS DECODED_INPUT
    FROM merged
    WHERE ABI IS NULL
),

all_transactions AS (
    SELECT * FROM decoded_cleaned
    UNION ALL
    SELECT * FROM no_abi
)

SELECT * FROM all_transactions