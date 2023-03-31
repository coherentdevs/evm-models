{{ config(materialized='table') }}
with extracted_arguments_table as (
    SELECT
        hex_to_int(BLOCK_NUMBER) as BLOCK_NUMBER,
        TRANSACTION_HASH,
        HASHABLE_SIGNATURE,
        INPUT,
        STATUS,
        METHOD_ID,
        extract_arguments(HASHABLE_SIGNATURE)[0] as extracted_arguments,
        extract_arguments(HASHABLE_SIGNATURE)[1] as number_of_arguments,
        extract_arguments(HASHABLE_SIGNATURE)[2] as contains_dynamic_arguments
    FROM {{ ref('raw_transactions_with_method_fragments_table') }}
    WHERE STATUS = '0x1' AND -- only valid transactions
    HASHABLE_SIGNATURE IS NOT NULL -- only transactions with a method id found
),

check_malformed_args_reseults AS (
    SELECT
        *,
        CASE
            WHEN contains_dynamic_arguments = True THEN check_malformed_args_present(extracted_arguments)
            ELSE False
        END AS malformed_arguments_present
    FROM extracted_arguments_table
)

SELECT * FROM check_malformed_args_reseults

