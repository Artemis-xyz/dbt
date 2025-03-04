{{
    config(
        materialized="incremental",
        unique_key=["address", "chain"],
        incremental_strategy="merge"
    )
}}

WITH combined_data AS (
    SELECT LOWER(address) AS address, name, namespace, chain, 
           1 AS table_priority
    FROM {{ ref('dim_usersubmittedcontracts') }} where chain is not null
    UNION
    SELECT LOWER(address) AS address, name, namespace, chain, 
           2 AS table_priority 
    FROM {{ ref('dim_scanner_contracts') }} where chain is not null
),
ranked_data AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY address, chain ORDER BY table_priority) AS row_rank
    FROM combined_data
)
SELECT address, name, namespace, chain, CURRENT_TIMESTAMP() AS last_updated
FROM ranked_data
WHERE row_rank = 1