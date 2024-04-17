{{ config(materialized="table") }}
select date, daa, chain
from {{ ref("fact_cosmoshub_daa") }}
