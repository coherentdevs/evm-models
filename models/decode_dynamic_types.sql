{{ config(materialized='table') }}
WITH decoded_dynamic_types AS (
    SELECT
        hex_to_int(BLOCK_NUMBER),
        TRANSACTION_HASH,
        HASHABLE_SIGNATURE,
        CONTRACT_ADDRESS,
        NAME,
        INPUT,
        decode_dynamic(EXTRACTED_ARGUMENTS, SUBSTRING(INPUT,11), 0) as decoded_result
    FROM {{ ref('extract_arguments') }}
    WHERE CONTAINS_DYNAMIC_ARGUMENTS = True
    LIMIT 10000000
)

SELECT *,
       check_decode_success(DECODED_RESULT) as decode_success
FROM decoded_dynamic_types