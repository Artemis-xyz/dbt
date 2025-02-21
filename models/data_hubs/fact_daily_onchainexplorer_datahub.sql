{{ config(materialized="table") }}

WITH last_30_days AS (
    SELECT
        contract_address,
        chain,
        namespace,
        date,
        dau
    FROM {{ ref('all_chains_gas_dau_txns_by_contract_v2') }}
    WHERE date >= DATEADD(DAY, -31, CURRENT_DATE)
),
last_30_days_name_augmented AS (
    SELECT
        l.*,
        ag.app_name,
        ag.icon
    FROM last_30_days l
    LEFT JOIN {{ ref('dim_all_apps_gold') }} ag
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
    WHERE namespace IS NULL AND contract_address IS NOT NULL AND TRIM(contract_address) <> ''
),
date_range AS (
    SELECT 
        DATEADD(DAY, -30 + (ROW_NUMBER() OVER (ORDER BY NULL) - 1), CURRENT_DATE - 1) AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 31))
),
unique_ids AS (
    SELECT DISTINCT unique_id, app_or_address, app_name, icon, type, chain
    FROM aggregated
),
all_dates AS (
    SELECT u.*, d.date
    FROM unique_ids u
    CROSS JOIN date_range d
),
final_aggregated AS (
    SELECT
        d.unique_id,
        d.app_or_address,
        d.app_name,
        d.icon,
        d.type,
        d.chain,
        d.date,
        COALESCE(ag.dau, 0) AS dau
    FROM all_dates d
    LEFT JOIN aggregated ag 
        ON d.unique_id = ag.unique_id 
        AND CAST(d.date AS DATE) = CAST(ag.date AS DATE)
),
ranked_unique_ids AS (
    SELECT 
        unique_id,
        chain,
        AVG(dau) AS avg_dau,
        ROW_NUMBER() OVER (
            PARTITION BY chain 
            ORDER BY AVG(dau) DESC, unique_id
        ) AS chain_rank,
        ROW_NUMBER() OVER (
            ORDER BY AVG(dau) DESC, unique_id
        ) AS global_rank
    FROM final_aggregated
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
),
clean_datahub AS (
    SELECT DISTINCT
        CONCAT(a.unique_id, '|', a.date) AS unique_id, 
        a.app_or_address, 
        a.app_name,
        a.icon,
        a.type,
        a.chain,
        a.date,
        a.dau,
        LAG(a.dau, 0) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS latest_dau,
        LAG(a.dau, 1) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS t_minus_one_dau,
        LAG(a.dau, 7) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS t_minus_seven_dau,
        LAG(a.dau, 30) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS t_minus_thirty_dau,
        f.chain_rank,
        f.global_rank
    FROM final_aggregated a
    JOIN filtered_ids f ON a.unique_id = f.unique_id
),
grouped_stats AS (
    SELECT
        app_or_address,
        chain,
        AVG(dau) AS dau_30d_avg,
        ARRAY_AGG(OBJECT_CONSTRUCT('date', date, 'val', dau)) WITHIN GROUP (ORDER BY date ASC) AS dau_30d_historical
    FROM clean_datahub
    GROUP BY app_or_address, chain
),
individual_stats AS (
    SELECT 
        app_or_address,
        app_name,
        icon,
        type,
        chain,
        date,
        dau,
        latest_dau,
        t_minus_one_dau,
        t_minus_seven_dau,
        t_minus_thirty_dau,
        CASE
            WHEN t_minus_one_dau = 0 THEN (latest_dau - t_minus_one_dau)
            ELSE (latest_dau - t_minus_one_dau) / t_minus_one_dau
        END AS dau_1d_change,
        CASE
            WHEN t_minus_seven_dau = 0 THEN (latest_dau - t_minus_seven_dau)
            ELSE (latest_dau - t_minus_seven_dau) / t_minus_seven_dau
        END AS dau_7d_change,
        CASE
            WHEN t_minus_thirty_dau = 0 THEN (latest_dau - t_minus_thirty_dau)
            ELSE (latest_dau - t_minus_thirty_dau) / t_minus_thirty_dau
        END AS dau_30d_change,
        chain_rank,
        global_rank
    FROM clean_datahub
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY app_or_address, chain 
        ORDER BY date DESC
    ) = 1
),
final_result AS (
    SELECT DISTINCT
        s.app_or_address,
        s.app_name,
        s.icon,
        s.type,
        s.chain,
        s.date,
        s.dau,
        gs.dau_30d_avg,
        s.dau_1d_change,
        s.dau_7d_change,
        s.dau_30d_change,
        gs.dau_30d_historical,
        s.chain_rank,
        s.global_rank
    FROM individual_stats s
    JOIN grouped_stats gs ON s.app_or_address = gs.app_or_address AND s.chain = gs.chain
) 
SELECT DISTINCT 
    CONCAT(app_or_address, '|', chain) AS unique_id,
    *
FROM final_result