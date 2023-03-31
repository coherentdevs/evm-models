{{ config(materialized='table') }}

WITH dynamic_fixed_aggregated AS
(
  SELECT * FROM {{ ref('decode_dynamic_types') }}
  UNION ALL
  SELECT * FROM {{ ref('decode_fixed_types') }}
)

SELECT * FROM dynamic_fixed_aggregated