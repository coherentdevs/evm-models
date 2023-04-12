{{ config(materialized='incremental', unique_key='block_hash') }}

{% if is_incremental() %}
    SELECT
        hex_to_int(DIFFICULTY) AS difficulty,
        EXTRA_DATA,
        hex_to_int(GAS_LIMIT) AS gas_limit,
        hex_to_int(GAS_USED) AS gas_used,
        BLOCK_HASH as block_hash,
        hex_to_int(BLOCK_NUMBER) AS block_number,
        LOGS_BLOOM,
        MINER,
        MIX_HASH,
        NONCE,
        PARENT_HASH,
        RECEIPTS_ROOT,
        SHA3_UNCLES,
        hex_to_int(SIZE) AS size,
        STATE_ROOT,
        hex_to_int(TIMESTAMP) AS timestamp,
        hex_to_int(TOTAL_DIFFICULTY) AS total_difficulty,
        TRANSACTIONS_ROOT,
        UNCLES,
        hex_to_int(BASE_FEE_PER_GAS) AS base_fee_per_gas
    FROM {{ source('ethereum_managed', 'blocks') }}
    WHERE to_number(SUBSTR(block_number, 3), repeat('X', length(SUBSTR(block_number, 3)))) > (SELECT MAX(CAST(block_number AS INTEGER)) FROM {{ this }}) -- this is the only change
{% else %}
    SELECT
        hex_to_int(DIFFICULTY) AS difficulty,
        EXTRA_DATA,
        hex_to_int(GAS_LIMIT) AS gas_limit,
        hex_to_int(GAS_USED) AS gas_used,
        BLOCK_HASH as block_hash,
        hex_to_int(BLOCK_NUMBER) AS block_number,
        LOGS_BLOOM,
        MINER,
        MIX_HASH,
        NONCE,
        PARENT_HASH,
        RECEIPTS_ROOT,
        SHA3_UNCLES,
        hex_to_int(SIZE) AS size,
        STATE_ROOT,
        hex_to_int(TIMESTAMP) AS timestamp,
        hex_to_int(TOTAL_DIFFICULTY) AS total_difficulty,
        TRANSACTIONS_ROOT,
        UNCLES,
        hex_to_int(BASE_FEE_PER_GAS) AS base_fee_per_gas
    FROM {{ source('ethereum_managed', 'blocks') }}
{% endif %}
