{{ config(materialized="incremental") }}

with tvl as (
    select
        *
    from
        {{ref('fact_defillama_protocol_tvls')}}
    where
        defillama_protocol_id = 5258
)
select
    date,
    tvl
from tvl
{% if not is_incremental() %}
    where date < '2025-05-01'
{% endif %}
{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}
