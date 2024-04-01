{{ config(materialized="table") }}

select date, unique_traders, app, category, chain
from {{ ref("fact_perpetual_protocol_unique_traders") }}
