{{ config(materialized="table") }}

select date, unique_traders, app, category, chain
from {{ ref("fact_synthetix_unique_traders") }}
