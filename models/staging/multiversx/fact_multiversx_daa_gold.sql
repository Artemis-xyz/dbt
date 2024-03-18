{{ config(materialized="table") }}
select date, daa, chain, source
from {{ ref("fact_multiversx_daa") }}
