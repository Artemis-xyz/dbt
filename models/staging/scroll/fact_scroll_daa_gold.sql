{{ config(materialized="table") }}
select date, daa, chain
from {{ ref("fact_scroll_daa") }}
