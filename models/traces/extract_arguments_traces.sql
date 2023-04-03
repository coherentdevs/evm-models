{{ config(materialized='table') }}
with extracted_arguments_table as (
    SELECT
        BLOCK_HASH as block_hash,
        BLOCK_NUMBER,
        GAS,
        GAS_USED,
        INPUT,
        OUTPUT,
        TRACE_HASH,
        TRANSACTION_HASH,
        VALUE,
        METHOD_ID,
        HASHABLE_SIGNATURE,
        extract_arguments(HASHABLE_SIGNATURE)[0] as extracted_arguments,
        extract_arguments(HASHABLE_SIGNATURE)[1] as number_of_arguments,
        extract_arguments(HASHABLE_SIGNATURE)[2] as contains_dynamic_arguments
    FROM {{ ref('raw_traces_with_method_fragments_table') }}
    WHERE HASHABLE_SIGNATURE IS NOT NULL -- only transactions with a method id found
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

