{{ config(materialized="incremental", unique_key="date") }}

with defillama_meteora_amm_fees as (
    SELECT
        date,
        fees
    FROM
        {{ref('fact_defillama_protocol_fees')}}
    WHERE
        defillama_protocol_id = 385
)

SELECT
    date,
    fees
FROM defillama_meteora_amm_fees
{% if not is_incremental() %}
    where date < '2025-05-01'
{% endif %}
{% if is_incremental() %}
    where defillama_meteora_amm_fees.date > (select dateadd('day', -3, max(date)) from {{ this }})
{% endif %}