{{
    config(
        materialized="view",
        database="common",
        schema="core",
        snowflake_warehouse="COMMON",
    )
}}

select 
    date,
    artemis_id,
    sectors,
    dau,
    txns,
    volume,
    fees,
    tvl,
    price,
    market_cap,
    fdmc,
    token_volume
from {{ ref("fact_protocol_datahub_gold") }}
where date < to_date(sysdate())
order by date desc, artemis_id asc