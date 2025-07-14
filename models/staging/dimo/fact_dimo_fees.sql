{{
    config(
        materialized="table",
        snowflake_warehouse="DIMO",
    )
}}

WITH price_data AS (
    {{ get_coingecko_price_with_latest('dimo') }}
),
monthly_inflow AS (
    SELECT
        block_timestamp::date AS date,
        SUM(TRY_CAST(amount AS DOUBLE)) AS inflow,
        SUM(
            CASE
                WHEN (TRY_CAST(amount AS DOUBLE)) BETWEEN 20
                AND 50 THEN (TRY_CAST(amount AS DOUBLE))
                ELSE 0
            END
        ) AS developer_license_purchase,
        SUM(
            CASE
                WHEN (TRY_CAST(amount AS DOUBLE)) = 0.5 THEN (TRY_CAST(amount AS DOUBLE))
                ELSE 0
            END
        ) AS transferring_dimo_out_of_account,
        SUM(
            CASE
                WHEN NOT (
                    (TRY_CAST(amount AS DOUBLE)) BETWEEN 20
                    AND 50
                )
                AND (TRY_CAST(amount AS DOUBLE)) <> 0.5 THEN (TRY_CAST(amount AS DOUBLE))
                ELSE 0
            END
        ) AS dcx_purchase
    FROM
        polygon_flipside.core.ez_token_transfers
    WHERE
        contract_address = lower('0xe261d618a959afffd53168cd07d12e37b26761db')
        AND to_address = lower('0x62b98e019e0d3e4A1Ad8C786202e09017Bd995e1')
    GROUP BY
        1
),
date_series AS (
    SELECT date FROM {{ ref('dim_date_spine')}}
    WHERE date between (SELECT MIN(date) FROM monthly_inflow) and to_date(sysdate())
)
SELECT
    ds.date,
    mi.developer_license_purchase,
    mi.transferring_dimo_out_of_account,
    mi.dcx_purchase,
    mi.inflow as fees_native,
    mi.inflow * pd.price AS fees,
    -- Running totals computed solely from the inflows (Revenue)
    SUM(COALESCE(mi.inflow, 0)) OVER (
        ORDER BY
            mi.date
    ) AS fees_native_cumulative,
    SUM(mi.inflow * pd.price) OVER (
        ORDER BY
            mi.date
    ) AS fees_cumulative
FROM
    date_series ds
    LEFT JOIN monthly_inflow mi using (date)
    LEFT JOIN price_data pd using (date)
ORDER BY
    ds.date