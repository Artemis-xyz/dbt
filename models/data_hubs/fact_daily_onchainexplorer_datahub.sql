{{ config(materialized="table") }}

WITH last_30_days AS (
    SELECT
        contract_address,
        chain,
        namespace,
        date,
        dau,
        real_users,
        total_gas_usd
    FROM {{ ref('all_chains_gas_dau_txns_by_contract_v2') }}
    WHERE date >= DATEADD(DAY, -30, CURRENT_DATE)
),
last_30_days_name_augmented AS (
    SELECT
        l.*,
        ag.app_name,
        ag.icon,
        ag.artemis_category_id,
        ag.artemis_sub_category_id
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
        MAX(artemis_category_id) AS category,
        MAX(artemis_sub_category_id) AS sub_category,
        chain,
        date,
        SUM(dau) AS dau,
        SUM(real_users) AS real_users,
        SUM(total_gas_usd) AS total_gas_usd
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
        NULL AS category,
        NULL AS sub_category,
        chain,
        date,
        dau,
        real_users,
        total_gas_usd
    FROM last_30_days_name_augmented
    WHERE namespace IS NULL AND contract_address IS NOT NULL AND TRIM(contract_address) <> ''
),
date_range AS (
    SELECT 
        DATEADD(DAY, -29 + (ROW_NUMBER() OVER (ORDER BY NULL) - 1), CURRENT_DATE - 1) AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 30))
),
unique_ids AS (
    SELECT DISTINCT unique_id, app_or_address, app_name, icon, type, category, sub_category, chain
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
        d.category,
        d.sub_category,
        d.chain,
        d.date,
        COALESCE(ag.dau, NULL) AS dau,
        COALESCE(ag.real_users, NULL) AS real_users,
        COALESCE(ag.total_gas_usd, NULL) AS total_gas_usd
    FROM all_dates d
    LEFT JOIN aggregated ag 
        ON d.unique_id = ag.unique_id 
        AND CAST(d.date AS DATE) = CAST(ag.date AS DATE)
),
ranked_unique_ids AS (
    SELECT 
        unique_id,
        app_name,
        chain,
        ROW_NUMBER() OVER (
            PARTITION BY chain 
            ORDER BY SUM(total_gas_usd) DESC NULLS LAST, unique_id
        ) AS chain_rank,
        ROW_NUMBER() OVER (
            ORDER BY SUM(total_gas_usd) DESC NULLS LAST, unique_id
        ) AS global_rank
    FROM final_aggregated
    WHERE app_or_address IS NOT NULL AND app_or_address != 'Unlabeled'
    GROUP BY unique_id, app_name, chain
),
filtered_ids AS (
    SELECT 
        unique_id, 
        chain_rank, 
        global_rank 
    FROM ranked_unique_ids 
    WHERE chain_rank <= 10000 OR global_rank <= 10000 OR app_name IS NOT NULL
),
clean_datahub AS (
    SELECT DISTINCT
        CONCAT(a.unique_id, '|', a.date) AS unique_id, 
        a.app_or_address, 
        a.app_name,
        a.icon,
        a.type,
        a.category,
        a.sub_category,
        a.chain,
        a.date,
        a.dau,
        LAG(a.dau, 0) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS latest_dau,
        LAG(a.dau, 1) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS t_minus_one_dau,
        LAG(a.dau, 7) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS t_minus_seven_dau,
        LAG(a.dau, 29) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS t_minus_thirty_dau,
        a.real_users,
        a.total_gas_usd,
        LAG(a.total_gas_usd, 0) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS latest_fees,
        LAG(a.total_gas_usd, 1) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS t_minus_one_fees,
        LAG(a.total_gas_usd, 7) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS t_minus_seven_fees,
        LAG(a.total_gas_usd, 29) OVER (PARTITION BY a.app_or_address, a.chain ORDER BY date ASC) AS t_minus_thirty_fees,
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
        AVG(real_users) AS real_users_30d_avg,
        SUM(total_gas_usd) AS fees_30d_total,
        ARRAY_AGG(OBJECT_CONSTRUCT('date', date, 'val', COALESCE(dau, 0))) 
            WITHIN GROUP (ORDER BY date ASC) AS dau_30d_historical
    FROM clean_datahub
    GROUP BY app_or_address, chain
),
individual_stats AS (
    SELECT 
        app_or_address,
        app_name,
        icon,
        type,
        category,
        sub_category,
        chain,
        date,
        dau,
        real_users,
        latest_dau,
        t_minus_one_dau,
        t_minus_seven_dau,
        t_minus_thirty_dau,
        CASE
            WHEN t_minus_one_dau IS NULL OR t_minus_one_dau = 0 THEN NULL
            ELSE (latest_dau - t_minus_one_dau) / t_minus_one_dau
        END AS dau_1d_change,
        CASE
            WHEN t_minus_seven_dau IS NULL OR t_minus_seven_dau = 0 THEN NULL
            ELSE (latest_dau - t_minus_seven_dau) / t_minus_seven_dau
        END AS dau_7d_change,
        CASE
            WHEN t_minus_thirty_dau IS NULL OR t_minus_thirty_dau = 0 THEN NULL
            ELSE (latest_dau - t_minus_thirty_dau) / t_minus_thirty_dau
        END AS dau_30d_change,
        total_gas_usd,
        latest_fees,
        t_minus_one_fees,
        t_minus_seven_fees,
        t_minus_thirty_fees,
        CASE
            WHEN t_minus_one_fees IS NULL OR t_minus_one_fees = 0 THEN NULL
            ELSE (latest_fees - t_minus_one_fees) / t_minus_one_fees
        END AS fees_1d_change,
        CASE
            WHEN t_minus_seven_fees IS NULL OR t_minus_seven_fees = 0 THEN NULL
            ELSE (latest_fees - t_minus_seven_fees) / t_minus_seven_fees
        END AS fees_7d_change,
        CASE
            WHEN t_minus_thirty_fees IS NULL OR t_minus_thirty_fees = 0 THEN NULL
            ELSE (latest_fees - t_minus_thirty_fees) / t_minus_thirty_fees
        END AS fees_30d_change,
        chain_rank,
        global_rank
    FROM clean_datahub
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY app_or_address, chain 
        ORDER BY date DESC
    ) = 1
),
fees_all_time AS (
    SELECT
        namespace AS app_or_address,
        'application' AS type,
        chain,
        SUM(total_gas_usd) AS fees_total
    FROM {{ ref('all_chains_gas_dau_txns_by_contract_v2') }}
    WHERE namespace IS NOT NULL
    GROUP BY namespace, chain

    UNION ALL

    SELECT
        contract_address AS app_or_address,
        'address' AS type,
        chain,
        SUM(total_gas_usd) AS fees_total
    FROM {{ ref('all_chains_gas_dau_txns_by_contract_v2') }}
    WHERE namespace IS NULL AND contract_address IS NOT NULL AND TRIM(contract_address) <> ''
    GROUP BY contract_address, chain
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
        s.real_users,
        gs.real_users_30d_avg,
        s.total_gas_usd AS fees,
        gs.fees_30d_total,
        s.fees_1d_change,
        s.fees_7d_change,
        s.fees_30d_change,
        s.chain_rank,
        s.global_rank,
        s.category,
        s.sub_category,
    FROM individual_stats s
    JOIN grouped_stats gs ON s.app_or_address = gs.app_or_address AND s.chain = gs.chain
),
chain_latest_data AS (
    SELECT 
        chain,
        MAX(date) AS latest_date
    FROM {{ ref('all_chains_gas_dau_txns_by_contract_v2') }}
    GROUP BY chain
),
outdated_chains AS (
    SELECT 
        chain
    FROM chain_latest_data
    WHERE latest_date < DATEADD(DAY, -1, TO_DATE(SYSDATE()))
),
merged_results AS (
    -- Use new data for chains with up-to-date information
    SELECT 
        CONCAT(s.app_or_address, '|', s.chain) AS unique_id,
        s.*,
        ft.fees_total
    FROM final_result s
    LEFT JOIN fees_all_time ft 
        ON s.app_or_address = ft.app_or_address 
            AND s.chain = ft.chain
    WHERE s.chain NOT IN (SELECT chain FROM outdated_chains)
    
    UNION ALL
    
    -- Use existing data for chains with outdated information
    SELECT *
    FROM {{ this }}
    WHERE chain IN (SELECT chain FROM outdated_chains)
),
all_apps_for_all_chains AS (
    SELECT DISTINCT 
        CONCAT(ac.namespace, '|', ac.chain) AS unique_id,
        ac.namespace AS app_or_address,
        ag.app_name,
        ag.icon,
        'application' AS type,
        ag.artemis_category_id AS category,
        ag.artemis_sub_category_id AS sub_category,
        ac.chain
    FROM {{ ref('all_chains_gas_dau_txns_by_contract_v2') }} ac
    LEFT JOIN {{ ref('dim_all_apps_gold') }} ag
        ON ac.namespace = ag.artemis_application_id
    WHERE ac.namespace IS NOT NULL
),
all_including_apps_with_no_30d_activity AS (
    SELECT 
        COALESCE(mr.unique_id, aac.unique_id) AS unique_id,
        COALESCE(mr.app_or_address, aac.app_or_address) AS app_or_address,
        COALESCE(mr.app_name, aac.app_name) AS app_name,
        COALESCE(mr.icon, aac.icon) AS icon,
        COALESCE(mr.type, aac.type) AS type,
        COALESCE(mr.chain, aac.chain) AS chain,
        mr.date,
        mr.dau,
        mr.dau_30d_avg,
        mr.dau_1d_change,
        mr.dau_7d_change,
        mr.dau_30d_change,
        mr.dau_30d_historical,
        mr.real_users,
        mr.real_users_30d_avg,
        mr.fees,
        mr.fees_30d_total,
        mr.fees_1d_change,
        mr.fees_7d_change,
        mr.fees_30d_change,
        mr.chain_rank,
        mr.global_rank,
        COALESCE(mr.category, aac.category) AS category,
        COALESCE(mr.sub_category, aac.sub_category) AS sub_category,
        mr.fees_total
    FROM merged_results mr
    FULL OUTER JOIN all_apps_for_all_chains aac
        ON mr.app_or_address = aac.app_or_address
        AND mr.chain = aac.chain
)
SELECT DISTINCT * FROM all_including_apps_with_no_30d_activity