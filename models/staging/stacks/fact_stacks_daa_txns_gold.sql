{{ config(materialized="table") }}

select date, chain, daa, txns
from {{ ref("fact_stacks_daa_txns") }}
