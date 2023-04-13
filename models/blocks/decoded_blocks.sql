{{ config(materialized='incremental', unique_key='block_hash') }}

{% if is_incremental() %}
    SELECT
        TRY_CAST(hex_to_int(DIFFICULTY) as FLOAT) AS difficulty,
        EXTRA_DATA,
        TRY_CAST(hex_to_int(GAS_LIMIT) as FLOAT) AS gas_limit,
        TRY_CAST(hex_to_int(GAS_USED) as FLOAT) AS gas_used,
        BLOCK_HASH as block_hash,
        TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) AS block_number,
        LOGS_BLOOM,
        MINER,
        MIX_HASH,
        TRY_CAST(hex_to_int(NONCE) as FLOAT) AS nonce,
        PARENT_HASH,
        RECEIPTS_ROOT,
        SHA3_UNCLES,
        TRY_CAST(hex_to_int(SIZE) as FLOAT) AS size,
        STATE_ROOT,
        TRY_CAST(hex_to_int(TIMESTAMP) as TIMESTAMP) AS timestamp,
        TRY_CAST(hex_to_int(TOTAL_DIFFICULTY) as FLOAT) AS total_difficulty,
        TRANSACTIONS_ROOT,
        UNCLES,
        TRY_CAST(hex_to_int(BASE_FEE_PER_GAS) as FLOAT) AS base_fee_per_gas
    FROM {{ source('ethereum_managed', 'blocks') }}
    WHERE to_number(SUBSTR(block_number, 3), repeat('X', length(SUBSTR(block_number, 3)))) > (SELECT MAX(CAST(block_number AS INTEGER)) FROM {{ this }}) -- this is the only change
{% else %}
    SELECT
        TRY_CAST(hex_to_int(DIFFICULTY) as FLOAT) AS difficulty,
        EXTRA_DATA,
        TRY_CAST(hex_to_int(GAS_LIMIT) as FLOAT) AS gas_limit,
        TRY_CAST(hex_to_int(GAS_USED) as FLOAT) AS gas_used,
        BLOCK_HASH as block_hash,
        TRY_CAST(hex_to_int(BLOCK_NUMBER) as FLOAT) AS block_number,
        LOGS_BLOOM,
        MINER,
        MIX_HASH,
        TRY_CAST(hex_to_int(NONCE) as FLOAT) AS nonce,
        PARENT_HASH,
        RECEIPTS_ROOT,
        SHA3_UNCLES,
        TRY_CAST(hex_to_int(SIZE) as FLOAT) AS size,
        STATE_ROOT,
        TRY_CAST(hex_to_int(TIMESTAMP) as TIMESTAMP) AS timestamp,
        TRY_CAST(hex_to_int(TOTAL_DIFFICULTY) as FLOAT) AS total_difficulty,
        TRANSACTIONS_ROOT,
        UNCLES,
        TRY_CAST(hex_to_int(BASE_FEE_PER_GAS) as FLOAT) AS base_fee_per_gas
    FROM {{ source('ethereum_managed', 'blocks') }}
{% endif %}
