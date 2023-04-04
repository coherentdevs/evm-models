{{ config(materialized='table') }}
WITH decoded_dynamic_types AS (
    SELECT
        BLOCK_HASH as block_hash,
        BLOCK_NUMBER AS block_number,
        GAS as gas,
        GAS_USED as gas_used,
        INPUT as input,
        OUTPUT as output,
        TRACE_HASH as trace_hash,
        TRANSACTION_HASH as transaction_hash,
        VALUE as value,
        HASHABLE_SIGNATURE AS hashable_signature,
        decode_dynamic(EXTRACTED_ARGUMENTS, SUBSTRING(INPUT,11), 0) AS decoded
    FROM {{ ref('extract_arguments_traces') }}
    WHERE CONTAINS_DYNAMIC_ARGUMENTS = True
    AND MALFORMED_ARGUMENTS_PRESENT = False
),

decoded_dynamic_types_parsed AS (
    SELECT
        BLOCK_HASH as block_hash,
        BLOCK_NUMBER AS block_number,
        GAS as gas,
        GAS_USED as gas_used,
        INPUT as input,
        OUTPUT as output,
        TRACE_HASH as trace_hash,
        TRANSACTION_HASH as transaction_hash,
        VALUE as value,
        HASHABLE_SIGNATURE AS hashable_signature,
        decoded[0] AS decoded_result,
        decoded[1] AS decode_success
    FROM decoded_dynamic_types
),

decoded_fixed_types AS (
    SELECT
        BLOCK_HASH as block_hash,
        BLOCK_NUMBER AS block_number,
        GAS as gas,
        GAS_USED as gas_used,
        INPUT as input,
        OUTPUT as output,
        TRACE_HASH as trace_hash,
        TRANSACTION_HASH as transaction_hash,
        VALUE as value,
        HASHABLE_SIGNATURE AS hashable_signature,
        decode_fixed(EXTRACTED_ARGUMENTS, SUBSTRING(INPUT,11)) AS decoded
    FROM {{ ref('extract_arguments_traces') }}
    WHERE CONTAINS_DYNAMIC_ARGUMENTS = False
    AND MALFORMED_ARGUMENTS_PRESENT = False
),

decoded_fixed_types_parsed AS (
    SELECT
        BLOCK_HASH as block_hash,
        BLOCK_NUMBER AS block_number,
        GAS as gas,
        GAS_USED as gas_used,
        INPUT as input,
        OUTPUT as output,
        TRACE_HASH as trace_hash,
        TRANSACTION_HASH as transaction_hash,
        VALUE as value,
        HASHABLE_SIGNATURE AS hashable_signature,
        decoded[0] AS decoded_result,
        decoded[1] AS decode_success
    FROM decoded_fixed_types
),

aggregated AS
(
  SELECT * FROM decoded_dynamic_types_parsed
  UNION ALL
  SELECT * FROM decoded_fixed_types_parsed
)

SELECT * FROM aggregated