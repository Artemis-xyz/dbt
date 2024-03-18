{{ config(materialized="table") }}
select date, daa, chain
from {{ ref("fact_zora_daa") }}
