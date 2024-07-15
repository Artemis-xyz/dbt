{{ config(materialized="table") }}
select date, daa, source, chain
from {{ ref("fact_bitcoin_daa") }}
