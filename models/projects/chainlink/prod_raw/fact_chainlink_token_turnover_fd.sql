{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_token_turnover_fd",
    )
}}

SELECT
    date,
    shifted_token_h24_volume_usd / (shifted_token_price_usd * 1000000000) as token_turnover_fdv
FROM pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
where coingecko_id = 'chainlink'
order by date desc