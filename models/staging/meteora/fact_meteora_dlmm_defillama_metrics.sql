{{ config(materialized="table") }}

with defillama_meteora_dlmm_tvl as (
    SELECT
        date,
        tvl
    FROM
        {{ref('fact_defillama_protocol_tvls')}}
    WHERE
        defillama_protocol_id = 4148
), defillama_meteora_dlmm_fees as (
    SELECT
        date,
        fees
    FROM
        {{ref('fact_defillama_protocol_fees')}}
    WHERE
        defillama_protocol_id = 4148
)

SELECT
    date,
    tvl,
    fees
from defillama_meteora_dlmm_tvl
left join defillama_meteora_dlmm_fees using(date)
{% if not is_incremental() %}
    where date < '2025-05-01'
{% endif %}
{% if is_incremental() %}
    where defillama_meteora_dlmm_tvl.date > (select max(date) from {{ this }})
{% endif %}