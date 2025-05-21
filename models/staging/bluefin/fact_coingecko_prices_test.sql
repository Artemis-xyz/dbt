{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

{{get_multiple_coingecko_price_with_latest('sui')}}