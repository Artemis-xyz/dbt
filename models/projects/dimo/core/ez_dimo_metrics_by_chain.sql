{{
    config(
        materialized="table",
        snowflake_warehouse="DIMO",
        database="dimo",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select
    date,
    'polygon' as chain,
    token_incentives_native,
    token_incentives,
    airdrop_native,
    airdrop,
    price,
    market_cap,
    fdmc,
    fees,
    fees_native,
    token_turnover_circulating,
    token_turnover_fdv,
    token_volume
from {{ ref("ez_dimo_metrics") }}