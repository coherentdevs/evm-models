{{ config(materialized='view') }}

SELECT
    *
FROM PC_DBT_DB.DBT_.METHOD_FRAGMENTS_TABLE_UNISWAP_SAMPLE
    LIMIT 10