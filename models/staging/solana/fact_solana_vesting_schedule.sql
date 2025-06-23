{{
    config(
        materialized="table",
        snowflake_warehouse="SOLANA",
    )
}}

WITH base_schedule AS (

    -- Community Reserve: linear monthly Apr 2020 – Jan 2021 (9 months)
    SELECT
        DATEADD(month, seq4(), DATE '2020-04-07') AS unlock_date,
        'Community Reserve' AS category,
        500e6 * .3889 / 9 AS amount_unlocked
    FROM TABLE(GENERATOR(ROWCOUNT => 9))

    UNION ALL

    -- Team: 50% cliff unlock Jan 7, 2021, 50% linearly over 24 months
    SELECT DATE '2021-01-07', 'Team', 500e6 * .1279 * 0.5
    UNION ALL
    SELECT
        DATEADD(month, seq4(), DATE '2021-02-07'),
        'Team',
        500e6 * .1279 * 0.5 / 24
    FROM TABLE(GENERATOR(ROWCOUNT => 24))

    UNION ALL

    -- Seed Round: full unlock Jan 7, 2021
    SELECT DATE '2021-01-07', 'Seed Round Investors', 500e6 * .1623

    UNION ALL

    -- Founding Sale: full unlock Jan 7, 2021
    SELECT DATE '2021-01-07', 'Founding Sale', 500e6 * .1292

    UNION ALL

    -- Validator Sale: full unlock Jan 7, 2021
    SELECT DATE '2021-01-07', 'Validator Sale', 500e6 * .0518

    UNION ALL

    -- Strategic Sale: full unlock Jan 7, 2021
    SELECT DATE '2021-01-07', 'Strategic Sale', 500e6 * .0188

    UNION ALL

    -- Coinlist Auction: full unlock at TGE (Mar 20, 2020)
    SELECT DATE '2020-03-20', 'Coinlist Auction Sale', 500e6 * .0164

    UNION ALL

    -- Foundation: 0.48% at TGE, rest linear to Jan 2021
    SELECT DATE '2020-03-20', 'Solana Foundation', 500e6 * .1046 * 0.0048
    UNION ALL
    SELECT
        DATEADD(month, seq4(), DATE '2020-04-01'),
        'Solana Foundation',
        500e6 * .1046 * (1 - 0.0048) / 9
    FROM TABLE(GENERATOR(ROWCOUNT => 9))
),

dates_and_categories AS (
    SELECT
        d.date,
        s.category
    FROM dim_date_spine d
    CROSS JOIN (SELECT DISTINCT category FROM base_schedule) s
    WHERE d.date between DATE '2020-03-20' and to_date(sysdate())
),

unlocks_by_day AS (
    SELECT
        d.date,
        d.category,
        COALESCE(b.amount_unlocked, 0) AS amount_unlocked
    FROM dates_and_categories d
    LEFT JOIN base_schedule b
        ON d.date = b.unlock_date AND d.category = b.category
),

final AS (
    SELECT
        date,
        category,
        amount_unlocked,
        SUM(amount_unlocked) OVER (
            PARTITION BY category ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_unlocked
    FROM unlocks_by_day
)

SELECT *
FROM final
