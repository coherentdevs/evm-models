{{ config(materialized='table') }}
WITH decoded_data AS (
    SELECT
        BLOCK_NUMBER,
        TRANSACTION_HASH,
        HASHABLE_SIGNATURE,
        INPUT,
        STATUS,
        decode_fixed(EXTRACTED_ARGUMENTS, SUBSTRING(INPUT,11)) as decoded
    FROM {{ ref('extract_arguments') }}
    WHERE CONTAINS_DYNAMIC_ARGUMENTS = FALSE
    AND MALFORMED_ARGUMENTS_PRESENT = FALSE
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