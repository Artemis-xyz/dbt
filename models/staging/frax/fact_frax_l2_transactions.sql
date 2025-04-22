{{ config(materialized="table") }}

select
    date,
    value as l2_txns
from {{source('PC_DBT_DB', 'fractal_L2')}}
