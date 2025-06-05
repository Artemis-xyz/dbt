{% macro get_pendle_swap_fees_for_chain_by_token(chain)%}

    -- Final aggregation
    SELECT
        date,
        chain,
        symbol,
        SUM(total_fees_usd) as fee_usd,
        SUM(total_fees) as fee_native,
        SUM(net_sy_out_usd) as volume_usd,
        SUM(net_sy_out) as volume_native,
        SUM(revenue_usd) as revenue_usd,
        SUM(revenue) as revenue_native,
        SUM(supply_side_fees_usd) as supply_side_fees_usd,
        SUM(supply_side_fees) as supply_side_fees_native
    FROM {{ ref('fact_pendle_' ~ chain ~ '_amm_swaps') }}
    GROUP BY 1, 2, 3

{% endmacro %}