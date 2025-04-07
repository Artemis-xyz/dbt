{{ config(materialized="table") }}

select 
    artemis_id as chain_name,
    symbol as chain_symbol,
    name as chain_display_name
from {{ ref("dim_chain") }}