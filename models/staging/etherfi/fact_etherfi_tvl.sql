{{ config(materialized="table") }}

with liquid as (
    SELECT
        *
    FROM
        {{ref('fact_defillama_protocol_tvls')}}
    WHERE
        defillama_protocol_id = 4429
),
stake as (
    SELECT
        *
    FROM
        {{ref('fact_defillama_protocol_tvls')}}
    WHERE
        defillama_protocol_id = 2626
)
SELECT
    coalesce(s.date, l.date) as date,
    s.tvl as stake_tvl,
    coalesce(l.tvl, 0) as liquid_tvl
FROM
    stake s
    LEFT JOIN liquid l ON l.date = s.date
WHERE
    s.date < to_date(sysdate())