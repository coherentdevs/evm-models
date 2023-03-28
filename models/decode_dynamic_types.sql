{{ config(materialized='table') }}
WITH decoded_dynamic_types AS (
    SELECT TRANSACTION_HASH,
           HASHABLE_SIGNATURE,
           CONTRACT_ADDRESS,
           NAME,
           INPUT,
           decode_dynamic(EXTRACTED_ARGUMENTS, INPUT) as decoded_result
    FROM {{ ref('extract_arguments') }}
    WHERE CONTAINS_DYNAMIC_ARGUMENTS = FALSE
)

SELECT *,
       check_decode_success(DECODED_RESULT) as decode_success
FROM decoded_fixed_types