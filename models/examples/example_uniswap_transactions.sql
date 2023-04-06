{{ config(materialized='table') }}
-- Example of a custom model that uses the uniswap contract address and function name to find all transactions that call the function
{% set uniswap_contract_address = '0xe592427a0aece92de3edee1f18e0157c05861564' %}
{% set function_name = 'multicall(bytes[])' %}
{% set block_number_min = 16000000 %}

WITH decoded_transactions AS (
    SELECT * FROM {{ ref('decoded_transactions') }}
    WHERE block_number > {{ block_number_min }}
),

uniswap_transactions AS (
    SELECT
        dt.BLOCK_NUMBER,
        dt.BLOCK_HASH,
        dt.TRANSACTION_HASH,
        dt.FROM_ADDRESS,
        dt.TO_ADDRESS,
        dt.VALUE,
        dt.DECODED_INPUT
    FROM decoded_transactions AS dt
    WHERE dt.TO_ADDRESS = '{{ uniswap_contract_address }}'
      AND dt.hashable_signature = '{{ function_name }}'
)

SELECT * FROM uniswap_transactions LIMIT 100
