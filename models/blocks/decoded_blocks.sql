{{ config(materialized='table') }}

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
FROM
    {{ source('ethereum_managed', 'blocks') }}
