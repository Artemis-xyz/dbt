{{
    config(
        materialized="table",
        snowflake_warehouse="DEBRIDGE",
        database="debridge",
        schema="core",
        alias="ez_metrics",
    )
}}

with bridge_volume_fees as (
    select 
        date
        , bridge_volume
        , ecosystem_revenue
        , bridge_txns
    from {{ ref("fact_debridge_fundamental_metrics") }}
)

select
    date
    , bridge_volume
    , ecosystem_revenue
    , bridge_txns
from bridge_volume_fees
where date < to_date(sysdate())
