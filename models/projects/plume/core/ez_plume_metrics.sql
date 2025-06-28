{{
    config(
        materialized="table",
        snowflake_warehouse="PLUME",
        database="plume",
        schema="core",
        alias="ez_metrics",
    )
}}

select
    date
    , txns
    , daa as dau
    , fees_native
    , fees
    , rwa_tvl
    , stablecoin_total_supply
from {{ ref("fact_plume_fundamental_metrics") }}
where date < to_date(sysdate())
