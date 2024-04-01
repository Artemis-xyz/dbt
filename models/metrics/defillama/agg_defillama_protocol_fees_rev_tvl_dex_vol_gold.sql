{{ config(materialized="table") }}

select date, defillama_protocol_id, fees, revenue, dex_volumes, tvl
from {{ ref("agg_defillama_protocol_fees_rev_tvl_dex_vol") }}
