{{
    config(
        materialized="table",
        snowflake_warehouse="ETHERFI",
        database="etherfi",
        schema="core",
        alias="ez_metrics",
    )
}}

SELECT
    date,
    'etherfi' as app,
    'DeFi' as category,
    sum(num_restaked_eth) as lrt_tvl_native,
    sum(amount_restaked_usd) as lrt_tvl,
    sum(num_restaked_eth_net_change) as lrt_tvl_native_net_change,
    sum(amount_restaked_usd_net_change) as lrt_tvl_net_change
from {{ ref('ez_etherfi_metrics_by_chain') }}
where date < to_date(sysdate())
group by 1, 2, 3

