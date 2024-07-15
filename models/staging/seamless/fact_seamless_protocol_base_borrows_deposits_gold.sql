{{ config(materialized="table") }}
select
    date,
    chain,
    'seamlessprotocol' as app,
    category,
    daily_borrows_usd,
    daily_supply_usd
from {{ ref("fact_seamless_protocol_base_borrows_deposits") }}
