{{ config(materialized="table") }}

select 
    artemis_id as chain_name,
    symbol as chain_symbol,
    name as chain_display_name,
    coingecko_id,
    defillama_chain_name,
    ecosystem_id
from {{ ref("dim_chain") }}