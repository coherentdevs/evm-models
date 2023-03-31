{{ config(materialized='table') }}

WITH method_fragments_count AS (
    SELECT
        METHOD_ID,
        HASHABLE_SIGNATURE,
        COUNT(*) AS frequency
    FROM {{ source('contracts', 'method_fragments') }}
    GROUP BY METHOD_ID, HASHABLE_SIGNATURE
),

method_fragments_max_count AS (
    SELECT
        METHOD_ID,
        MAX(frequency) AS max_frequency
    FROM method_fragments_count
    GROUP BY METHOD_ID
)

SELECT
    mfc.METHOD_ID,
    mfc.HASHABLE_SIGNATURE,
    mfc.frequency
FROM method_fragments_count mfc
JOIN method_fragments_max_count mfmc
ON mfc.METHOD_ID = mfmc.METHOD_ID AND mfc.frequency = mfmc.max_frequency
