{{ config(materialized='table') }}
WITH decoded AS (
    SELECT *
    FROM {{ ref('decode_traces') }}
),

no_hashable_signature as (
    SELECT
        block_hash,
        block_number,
        error,
        from_address,
        gas,
        gas_used,
        trace_index,
        input,
        output,
        parent_hash,
        revert_reason,
        to_address,
        trace_hash,
        transaction_hash,
        type,
        value,
        NULL AS hashable_signature,
        NULL AS decoded_input
    FROM {{ ref('raw_traces_with_method_fragments_table') }}
    WHERE hashable_signature IS NULL
),

--- these traces are able to be decoded, JOIN them with the decoded table
hashable_signature_found as (
    SELECT *
    FROM {{ ref('raw_traces_with_method_fragments_table') }}
    WHERE hashable_signature IS NOT NULL
),

decoded_joined as (
    SELECT
        raw_traces.block_hash,
        raw_traces.block_number,
        raw_traces.error,
        raw_traces.from_address,
        raw_traces.gas,
        raw_traces.gas_used,
        raw_traces.trace_index,
        raw_traces.input,
        raw_traces.output,
        raw_traces.parent_hash,
        raw_traces.revert_reason,
        raw_traces.to_address,
        raw_traces.trace_hash,
        raw_traces.transaction_hash,
        raw_traces.type,
        raw_traces.value,
        decoded_traces.hashable_signature,
        decoded_traces.decoded_result as decoded_input

    FROM hashable_signature_found raw_traces
    LEFT JOIN decoded decoded_traces
    ON raw_traces.trace_hash = decoded_traces.trace_hash AND
    raw_traces.transaction_hash = decoded_traces.transaction_hash
),

full_table AS
(
  SELECT * FROM no_hashable_signature
  UNION ALL
  SELECT * FROM decoded_joined
)

SELECT * FROM full_table

