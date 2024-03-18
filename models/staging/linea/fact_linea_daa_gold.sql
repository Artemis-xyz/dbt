{{ config(materialized="table") }}
select date, daa, chain
from {{ ref("fact_linea_daa") }}
