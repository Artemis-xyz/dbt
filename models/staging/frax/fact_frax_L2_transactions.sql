{{ config(materialized="table") }}

select
    date,
    value as l2_txns
from {{ref('fractal_L2')}}