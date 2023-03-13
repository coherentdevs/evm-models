{{ config(materialized='view') }}

SELECT
    BLOCK_HASH,
    hex_to_int(BLOCK_NUMBER) as BLOCK_NUMBER,
    decode_transaction_input(abi, input) as INPUT,
    TO_ADDRESS
FROM PC_DBT_DB.DBT_.UNISWAP_TRANSACTIONS as ut
INNER JOIN PC_DBT_DB.DBT_.CONTRACTS_TABLE as ct ON ct.ADDRESS=ut.TO_ADDRESS
    LIMIT 10