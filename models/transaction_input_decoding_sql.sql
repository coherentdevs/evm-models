{{ config(materialized='table') }}

SELECT *,
       decode_input(INPUT, HASHABLE_SIGNATURE) as decoded_result
FROM {{ ref('raw_transactions_with_method_fragments_table') }} LIMIT 100000

-- SELECT
--   *,
--   check_decode_success(decoded_result) as decode_success
-- FROM decoded_input_results