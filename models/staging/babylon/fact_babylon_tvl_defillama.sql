{{ config(materialized="incremental") }}

with tvl as (
    SELECT
        *
    FROM
        {{ref('fact_defillama_protocol_tvls')}}
    WHERE
        defillama_protocol_id = 5258
)
SELECT
    date,
    tvl
FROM tvl
WHERE date < '2025-05-01'
{% if is_incremental() %}
    or tvl.date > (select max(date) from {{ this }})
{% endif %}
