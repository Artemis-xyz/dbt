{{
    config(
        materialized="table",
        alias="fact_babylon_metrics",
    )
}}

WITH parsed_data AS (
  SELECT
    DATE(EXTRACTION_DATE) as date,
    EXTRACTION_DATE,
    PARSE_JSON(SOURCE_JSON):data as json_data
  FROM {{ source('PROD_LANDING', 'raw_babylon_metrics') }}
),
ranked_data AS (
  SELECT
    date,
    EXTRACTION_DATE,
    json_data,
    ROW_NUMBER() OVER (PARTITION BY date ORDER BY EXTRACTION_DATE DESC) as rn
  FROM parsed_data
)
, latest_data as (
  SELECT
    date,
    json_data:active_delegations::FLOAT as active_delegations,
    json_data:active_tvl::FLOAT as active_tvl,
    json_data:active_tvl::FLOAT / 1e8 as active_tvl_btc,
    json_data:btc_price_usd::FLOAT as btc_price_usd,
    active_tvl_btc * btc_price_usd as active_tvl_usd,
    json_data:pending_tvl::FLOAT as pending_tvl,
    json_data:total_delegations::FLOAT as total_delegations,
    json_data:total_stakers::FLOAT as total_stakers,
    json_data:total_tvl::FLOAT as total_tvl,
    json_data:unconfirmed_tvl::FLOAT as unconfirmed_tvl
  FROM ranked_data
  WHERE rn = 1
  ORDER BY date
)
,
date_spine AS (
  SELECT date
  FROM {{ ref('dim_date_spine') }}
  WHERE date BETWEEN (SELECT MIN(date) FROM latest_data) AND TO_DATE(SYSDATE())
),
front_filled_data AS (
  SELECT
    ds.date,
    COALESCE(
      ld.active_delegations,
      LAST_VALUE(ld.active_delegations) IGNORE NULLS OVER (ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0
    ) AS active_delegations,
    COALESCE(
      ld.active_tvl,
      LAST_VALUE(ld.active_tvl) IGNORE NULLS OVER (ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0
    ) AS active_tvl,
    COALESCE(
      ld.active_tvl_btc,
      LAST_VALUE(ld.active_tvl_btc) IGNORE NULLS OVER (ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0
    ) AS active_tvl_btc,
    COALESCE(
      ld.btc_price_usd,
      LAST_VALUE(ld.btc_price_usd) IGNORE NULLS OVER (ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0
    ) AS btc_price_usd,
    COALESCE(
      ld.active_tvl_usd,
      LAST_VALUE(ld.active_tvl_usd) IGNORE NULLS OVER (ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0
    ) AS active_tvl_usd,
    COALESCE(
      ld.pending_tvl,
      LAST_VALUE(ld.pending_tvl) IGNORE NULLS OVER (ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0
    ) AS pending_tvl,
    COALESCE(
      ld.total_delegations,
      LAST_VALUE(ld.total_delegations) IGNORE NULLS OVER (ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0
    ) AS total_delegations,
    COALESCE(
      ld.total_stakers,
      LAST_VALUE(ld.total_stakers) IGNORE NULLS OVER (ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0
    ) AS total_stakers,
    COALESCE(
      ld.total_tvl,
      LAST_VALUE(ld.total_tvl) IGNORE NULLS OVER (ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0
    ) AS total_tvl,
    COALESCE(
      ld.unconfirmed_tvl,
      LAST_VALUE(ld.unconfirmed_tvl) IGNORE NULLS OVER (ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0
    ) AS unconfirmed_tvl
  FROM date_spine ds
  LEFT JOIN latest_data ld ON ds.date = ld.date
)

SELECT *
FROM front_filled_data
ORDER BY date


