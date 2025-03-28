{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="JUPITER",
    )
}}

with
    all_trade_data as (
        SELECT 
            date,
            'limit_order' as trade_type,
            fees,
            fees as revenue, -- Jup retains 100% of limit order fees
            0 as supply_side_revenue,
            volume,
            txns,
            NULL as dau -- Have not calculated dau for limit order yet
        FROM {{ ref("fact_jupiter_limit_order_fees_silver") }}
        UNION ALL
        SELECT 
            date,
            'dca' as trade_type,
            fees,
            fees as revenue, -- Jup retains 100% of DCA fees
            0 as supply_side_revenue,
            volume,
            txns,
            NULL as dau -- Have not calculated dau for DCA yet
        FROM {{ ref("fact_jupiter_dca_fees_silver") }}
        UNION ALL
        SELECT 
            date,
            'perps' as trade_type,
            fees,
            fees * 0.25 as revenue, -- Jup retains 25% of perps fees
            fees * 0.75 as supply_side_revenue, -- JLP retains 75% of perps fees
            volume,
            txns,
            traders as dau
        FROM {{ ref("fact_jupiter_perps_silver") }}
        UNION ALL
        SELECT 
            date,
            'aggregator' as trade_type,
            swap_fees,
            CASE 
                WHEN swap_type = 'Ultra' 
                    THEN swap_fees  -- Jup retains 100% of Ultra fees
                WHEN swap_type = 'Referral' AND date < '2025-01-14' -- 2.5% referral fee which has been around since launch but stopped in early January 2025
                    THEN swap_fees * 0.025 -- Jup retains 2.5% of referral fees
            END as revenue,
            0 as supply_side_revenue,
            volume,
            swap_count as txns,
            dau
        FROM {{ ref("fact_jupiter_swap_metrics") }}
    )
SELECT
     date,
     trade_type,
     sum(fees) as fees,
     sum(revenue) as revenue,
     sum(supply_side_revenue) as supply_side_revenue,
     sum(volume) as volume,
     sum(txns) as txns,
     sum(dau) as dau
FROM
    all_trade_data
GROUP BY 1,2