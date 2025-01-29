{{ config(materialized="table") }}

with chains AS (
    select distinct chain from dim_all_addresses_labeled_gold
)
select 
    c.chain as chain_name,
    dc.symbol as chain_symbol,
    dc.name as chain_display_name
from chains c
left join dim_chain dc
on c.chain = dc.artemis_id