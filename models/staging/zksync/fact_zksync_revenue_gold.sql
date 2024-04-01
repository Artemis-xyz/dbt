{{ config(materialized="table") }}
select date, revenue, chain
from {{ ref("fact_zksync_revenue") }}
