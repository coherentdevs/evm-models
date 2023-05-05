{{ config(materialized='incremental', unique_key='block_hash') }}

{% if is_incremental() %}
    SELECT
        TRY_CAST(hex_to_int(DIFFICULTY) as FLOAT) as DIFFICULTY,
        EXTRA_DATA,
        TRY_CAST(hex_to_int(GAS_LIMIT) as FLOAT) as GAS_LIMIT,
        TRY_CAST(hex_to_int(GAS_USED) as FLOAT) as GAS_USED,
        BLOCK_HASH,
        TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) as BLOCK_NUMBER,
        LOGS_BLOOM,
        MINER,
        MIX_HASH,
        TRY_CAST(hex_to_int(NONCE) as FLOAT) AS nonce,
        PARENT_HASH,
        RECEIPTS_ROOT,
        SHA3_UNCLES,
        TRY_CAST(hex_to_int(SIZE) as FLOAT) as SIZE,
        STATE_ROOT,
        TRY_CAST(hex_to_int(TIMESTAMP) as TIMESTAMP) as TIMESTAMP,
        TRY_CAST(hex_to_int(TOTAL_DIFFICULTY) as FLOAT) as TOTAL_DIFFICULTY,
        TRANSACTIONS_ROOT,
        UNCLES
    FROM {{ source(var('optimism_raw_database'), 'blocks') }}
    WHERE to_number(SUBSTR(block_number, 3), repeat('X', length(SUBSTR(block_number, 3)))) > (SELECT MAX(block_number) FROM {{ this }}) -- this is the only change
{% else %}
    SELECT
        TRY_CAST(hex_to_int(DIFFICULTY) as FLOAT) as DIFFICULTY,
        EXTRA_DATA,
        TRY_CAST(hex_to_int(GAS_LIMIT) as FLOAT) as GAS_LIMIT,
        TRY_CAST(hex_to_int(GAS_USED) as FLOAT) as GAS_USED,
        BLOCK_HASH,
        TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) as BLOCK_NUMBER,
        LOGS_BLOOM,
        MINER,
        MIX_HASH,
        TRY_CAST(hex_to_int(NONCE) as FLOAT) AS nonce,
        PARENT_HASH,
        RECEIPTS_ROOT,
        SHA3_UNCLES,
        TRY_CAST(hex_to_int(SIZE) as FLOAT) as SIZE,
        STATE_ROOT,
        TRY_CAST(hex_to_int(TIMESTAMP) as TIMESTAMP) as TIMESTAMP,
        TRY_CAST(hex_to_int(TOTAL_DIFFICULTY) as FLOAT) as TOTAL_DIFFICULTY,
        TRANSACTIONS_ROOT,
        UNCLES
    FROM {{ source(var('optimism_raw_database'), 'blocks') }}
{% endif %}
