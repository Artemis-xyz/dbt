{{ config(materialized="table", snowflake_warehouse="CURVE_SM") }}
select date, chain, category, app, trading_volume, fees, tvl, unique_traders
from {{ ref("fact_curve_trading_vol_trading_fees_trading_revenue_ethereum") }}
full outer join {{ ref("fact_curve_tvl_ethereum") }} using (date, chain, category, app)
full outer join
    {{ ref("fact_curve_unique_traders_ethereum") }} using (date, chain, category, app)
