{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_cumulative_sums"
    )
}}

SELECT
    wp.*,
    SUM(wp.value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS cumulative_ale_token_value,
    SUM(wp.dai_value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS cumulative_ale_dai_value,
    SUM(wp.eth_value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS cumulative_ale_eth_value,
    m2m.price * SUM(wp.value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS dai_value_if_converted_all_once,
    m2m.price/wp.eth_price * SUM(wp.value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS eth_value_if_converted_all_once,
    m2m.price * SUM(wp.value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) - SUM(wp.dai_value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS dai_m2m,
    m2m.price/wp.eth_price * SUM(wp.value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) - SUM(wp.eth_value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS eth_m2m
FROM {{ ref('fact_with_prices') }} wp
LEFT JOIN {{ ref('fact_m2m_levels') }} m2m
    ON wp.token = m2m.token
    AND DATE_TRUNC('day', wp.ts) = DATE_TRUNC('day', m2m.ts)
    -- AND EXTRACT(HOUR FROM wp.ts) = EXTRACT(HOUR FROM m2m.ts)