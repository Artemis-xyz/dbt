{{ config(materialized="table") }}
select date, daa, chain, source
from {{ ref("fact_cosmoshub_daa") }}
