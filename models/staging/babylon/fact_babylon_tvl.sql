{{ config(materialized="table") }}

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
WHERE date < to_date(sysdate())