{{
    config(
        materialized='table',
        snowflake_warehouse='STABLECOIN_V2_LG'
    )
}}

WITH max_date AS (
    SELECT
        MAX(date) AS max_date
    FROM {{ ref('agg_daily_stablecoin_breakdown_with_labels') }}
), base AS (
    -- Child Rows
    SELECT
        date,
        LOWER(symbol) as parent_value,
        LOWER(chain) as child_value,
        LOWER(symbol) AS parent,
        SUM(stablecoin_daily_txns) AS stablecoin_daily_transfers,
        COUNT(DISTINCT case when stablecoin_daily_txns > 0 then address end) AS stablecoin_dau,
        SUM(stablecoin_supply) AS stablecoin_supply,
        SUM(stablecoin_transfer_volume) AS stablecoin_transfer_volume
    FROM {{ ref('agg_daily_stablecoin_breakdown_with_labels') }}
    WHERE
        (
            date = (SELECT max_date FROM max_date)
            OR date = DATEADD(day, -30, (SELECT max_date FROM max_date))
        )
        
    GROUP BY
        date, parent_value, child_value
    UNION ALL

    -- Parent Rows
    SELECT
        date,
        LOWER(symbol) as parent_value,
        NULL AS child_value,
        NULL AS parent,
        SUM(stablecoin_daily_txns) AS stablecoin_daily_transfers,
        COUNT(DISTINCT CASE WHEN stablecoin_daily_txns > 0 THEN address END) AS stablecoin_dau,
        SUM(stablecoin_supply) AS stablecoin_supply,
        SUM(stablecoin_transfer_volume) AS stablecoin_transfer_volume
    FROM {{ ref('agg_daily_stablecoin_breakdown_with_labels') }}
    WHERE
        (
            date = (SELECT max_date FROM max_date)
            OR date = DATEADD(day, -29, (SELECT max_date FROM max_date))
        )
        
    GROUP BY
        date, parent_value
), agg AS (
    SELECT
        date,
        LOWER(parent_value) AS parent_value,
        LOWER(child_value) AS child_value,
        ARRAY_AGG(child_value) AS path_arr,
        MAX(parent) AS parent,
        MAX(stablecoin_daily_transfers) AS stablecoin_daily_transfers,
        MAX(stablecoin_dau) AS stablecoin_dau,
        MAX(stablecoin_supply) AS stablecoin_supply,
        MAX(stablecoin_transfer_volume) AS stablecoin_transfer_volume
    FROM base
    GROUP BY
        date, parent_value, child_value
), ranked AS (
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY parent_value, child_value ORDER BY date DESC) AS row_num,
    LAG(stablecoin_daily_transfers) OVER (PARTITION BY parent_value, child_value ORDER BY date) AS prev_stablecoin_daily_transfers,
    LAG(stablecoin_dau) OVER (PARTITION BY parent_value, child_value ORDER BY DATE) AS prev_stablecoin_dau,
    LAG(stablecoin_supply) OVER (PARTITION BY parent_value, child_value ORDER BY DATE) AS prev_stablecoin_supply,
    LAG(stablecoin_transfer_volume) OVER (PARTITION BY parent_value, child_value ORDER BY DATE) AS prev_stablecoin_transfer_volume
FROM agg
), calculated_changes AS (
SELECT
    *,
    case
        when prev_stablecoin_daily_transfers is null or prev_stablecoin_daily_transfers = 0 then 0
        else (stablecoin_daily_transfers - prev_stablecoin_daily_transfers) / prev_stablecoin_daily_transfers * 100
    end AS stablecoin_daily_transfers_pct_chg,
    case
        when prev_stablecoin_dau is null or prev_stablecoin_dau = 0 then 0
        else (stablecoin_dau - prev_stablecoin_dau) / prev_stablecoin_dau * 100
    end AS stablecoin_dau_pct_chg,
    case
        when prev_stablecoin_supply is null or prev_stablecoin_supply = 0 then 0
        else (stablecoin_supply - prev_stablecoin_supply) / prev_stablecoin_supply * 100
    end AS stablecoin_supply_pct_chg,
    case
        when prev_stablecoin_transfer_volume is null or prev_stablecoin_transfer_volume = 0 then 0
        else (stablecoin_transfer_volume - prev_stablecoin_transfer_volume) / prev_stablecoin_transfer_volume * 100
    end AS stablecoin_transfer_volume_pct_chg
FROM ranked
)
SELECT
    COALESCE(child_value, parent_value) AS value,
    ARRAY_CAT([parent_value], ARRAY_SORT(path_arr)) AS path,
    parent,
    stablecoin_supply,
    stablecoin_supply_pct_chg,
    stablecoin_transfer_volume,
    stablecoin_transfer_volume_pct_chg,
    stablecoin_daily_transfers,
    stablecoin_daily_transfers_pct_chg,
    stablecoin_dau,
    stablecoin_dau_pct_chg
FROM calculated_changes
WHERE row_num = 1 and (round(stablecoin_supply, 1) > 0 or round(stablecoin_transfer_volume, 1) > 0 or round(stablecoin_daily_transfers, 1) > 0 or round(stablecoin_dau, 1) > 0)