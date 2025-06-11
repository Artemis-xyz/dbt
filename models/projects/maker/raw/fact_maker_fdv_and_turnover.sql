{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_fdv_and_turnover",
    )
}}


SELECT
    date,
    shifted_token_h24_volume_usd as token_volume,
    shifted_token_price_usd * 1005577 as fully_diluted_market_cap,
    shifted_token_h24_volume_usd / shifted_token_market_cap as token_turnover_circulating,
    shifted_token_h24_volume_usd / fully_diluted_market_cap as token_turnover_fdv
FROM pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
where coingecko_id = 'maker'
and shifted_token_market_cap > 0
and fully_diluted_market_cap > 0
order by date desc