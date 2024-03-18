{{ config(materialized="table") }}

select date, defillama_chain_name, dex_volumes, tvl
from {{ ref("agg_defillama_chain_fees_rev_tvl_dex_vol") }}
