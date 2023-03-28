{{ config(materialized='table') }}

SELECT
    hex_to_int(BLOCK_NUMBER) as BLOCK_NUMBER,
    TRANSACTION_HASH,
    HASHABLE_SIGNATURE,
    CONTRACT_ADDRESS,
    NAME,
    INPUT,
    extract_arguments(HASHABLE_SIGNATURE)[0] as extracted_arguments,
    extract_arguments(HASHABLE_SIGNATURE)[1] as number_of_arguments,
    extract_arguments(HASHABLE_SIGNATURE)[2] as contains_dynamic_arguments
FROM {{ ref('raw_transactions_with_method_fragments_table') }} LIMIT 100000000
