{{ config(materialized="table") }}
select date, daa, chain, source
from {{ ref("fact_fantom_daa") }}
