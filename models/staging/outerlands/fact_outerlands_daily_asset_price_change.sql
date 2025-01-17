{{
    config(
        materialized="table",
        snowflake_warehouse="outerlands"
    )
}}

-- depends_on: {{ ref('fact_coingecko_token_date_adjusted_gold') }}
-- depends_on: {{ ref('fact_coingecko_token_realtime_data') }}

{%- set sigma_assets = dbt_utils.get_query_results_as_dict(
    "SELECT 
        artemis_id,
        coingecko_id,
        DATE(CONVERT_TIMEZONE('UTC', added_date)) as added_date,
        DATE(CONVERT_TIMEZONE('UTC', deleted_date)) as deleted_date,
     FROM " ~ source('SIGMA', 'dim_outerlands_fundamental_index_assets')
) -%}

{{ '\n' }}WITH -- force new line because of how dbt compiles the depends_on comments above
    raw_price_data AS (
        {%- for i in range(sigma_assets['COINGECKO_ID'] | length) %}
        {%- set coingecko_id = sigma_assets['COINGECKO_ID'][i] %}
        
            select date as date, coingecko_id, shifted_token_price_usd as price
            from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
            where
                coingecko_id = '{{coingecko_id}}'
                and date < dateadd(day, -1, to_date(sysdate()))
            
            union
            select dateadd('day', -1, to_date(sysdate())) as date, token_id as coingecko_id, token_current_price as price
            from {{ ref("fact_coingecko_token_realtime_data") }}
            where token_id = '{{coingecko_id}}'

            union
            select to_date(sysdate()) as date, token_id as coingecko_id, token_current_price as price
            from {{ ref("fact_coingecko_token_realtime_data") }}
            where token_id = '{{coingecko_id}}'
        
        {%- if not loop.last %}
        {{ '\n' }}
        UNION ALL
        {%- endif %}
        {%- endfor %}
    )
, price_data AS (
    SELECT
        date,
        coingecko_id,
        price,
        LAG(price) OVER (PARTITION BY coingecko_id ORDER BY date) as previous_price
    FROM
        raw_price_data
)
SELECT
    date,
    coingecko_id,
    price,
    previous_price,
    CASE
        WHEN previous_price IS NOT NULL AND previous_price != 0
        THEN (price - previous_price) / previous_price * 100
        ELSE NULL
    END AS daily_percent_change
FROM
    price_data
ORDER BY
    date,
    coingecko_id