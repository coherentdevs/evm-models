{{ config(materialized='table') }}
WITH decoded_data AS (
    SELECT
        BLOCK_NUMBER AS block_number,
        TRANSACTION_HASH AS transaction_hash,
        HASHABLE_SIGNATURE AS hashable_signature,
        INPUT AS input,
        STATUS,
        decode_dynamic(EXTRACTED_ARGUMENTS, SUBSTRING(INPUT,11), 0) AS decoded
    FROM {{ ref('extract_arguments') }}
    WHERE CONTAINS_DYNAMIC_ARGUMENTS = True
    AND MALFORMED_ARGUMENTS_PRESENT = False
)

SELECT
    block_number,
    transaction_hash,
    hashable_signature,
    input,
    STATUS,
    decoded[0] AS decoded_result,
    decoded[1] AS decode_success
FROM decoded_data
