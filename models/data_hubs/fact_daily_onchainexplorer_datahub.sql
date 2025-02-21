{{ config(materialized="table") }}

WITH last_30_days AS (
    SELECT
        contract_address,
        chain,
        namespace,
        date,
        dau
    FROM {{ ref("all_chains_gas_dau_txns_by_contract_v2") }}
    WHERE date >= DATEADD(DAY, -30, CURRENT_DATE)
),
last_30_days_name_augmented AS (
    SELECT
        l.*,
        ag.app_name,
        ag.icon
    FROM last_30_days l
    LEFT JOIN {{ ref("dim_all_apps_gold") }} ag
     ON l.namespace = ag.artemis_application_id
),
aggregated AS (
    SELECT
        CONCAT('__null__', '|', namespace, '|', chain) AS unique_id,
        namespace AS app_or_address,
        app_name,
        icon,
        'application' AS type,
        chain,
        date,
        AVG(dau) AS dau
    FROM last_30_days_name_augmented
    WHERE namespace IS NOT NULL
    GROUP BY namespace, app_name, icon, chain, date

    UNION ALL

    SELECT
        CONCAT(contract_address, '|', '__null__', '|', chain) AS unique_id,
        contract_address AS app_or_address,
        NULL AS app_name,
        NULL AS icon,
        'address' AS type,
        chain,
        date,
        dau
    FROM last_30_days_name_augmented
    WHERE namespace IS NULL
),
ranked_unique_ids AS (
    SELECT 
        unique_id,
        chain,
        AVG(dau) AS avg_dau,
        -- Unique per-chain ranking with tiebreaker
        ROW_NUMBER() OVER (
            PARTITION BY chain 
            ORDER BY AVG(dau) DESC, unique_id
        ) AS chain_rank,
        -- Unique global ranking with tiebreaker
        ROW_NUMBER() OVER (
            ORDER BY AVG(dau) DESC, unique_id
        ) AS global_rank
    FROM aggregated
    WHERE app_or_address IS NOT NULL AND app_or_address != 'Unlabeled'
    GROUP BY unique_id, chain
),
filtered_ids AS (
    SELECT 
        unique_id, 
        chain_rank, 
        global_rank 
    FROM ranked_unique_ids 
    WHERE chain_rank <= 10000 OR global_rank <= 10000
)
SELECT DISTINCT
    CONCAT(a.unique_id, '|', a.date) AS unique_id, 
    a.app_or_address, 
    a.app_name,
    a.icon,
    a.type,
    a.chain,
    a.date,
    a.dau,
    f.chain_rank,
    f.global_rank
FROM aggregated a
JOIN filtered_ids f ON a.unique_id = f.unique_id