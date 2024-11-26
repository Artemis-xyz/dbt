{{
    config(
        materialized="table",
        snowflake_warehouse="ORCA",
        database="orca",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select date,'solana' as chain, 'orca' as protocol, trading_volume, unique_traders, number_of_swaps
from {{ ref("fact_orca_trading_metrics") }}
where date < to_date(sysdate())
