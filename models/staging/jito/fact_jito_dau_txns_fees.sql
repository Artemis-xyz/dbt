{{
    config(
        materialized='incremental',
        unique_key='day',
        snowflake_warehouse='jito'
    )
}}
with prices as (
    SELECT
        date(hour) as date
        , avg(price) as price
    FROM solana_flipside.price.ez_prices_hourly
    where is_native = 'True'
    GROUP BY 1
)

SELECT 
    date_trunc('day', t.block_timestamp) as day
    , sum(t.amount * p.price) as fees
    , sum(t.amount * p.price) * 0.05 as revenue
    , sum(t.amount * p.price) * 0.95 as supply_side_fees
    , count(*) as txns
    , count(distinct t.tx_from) as dau
FROM {{ source('SOLANA_FLIPSIDE', 'fact_transfers') }} t
LEFT JOIN prices p on p.date = date_trunc('day',t.block_timestamp)
WHERE tx_to IN ('96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5' -- all the tip payment accounts 
                ,'HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe'
                ,'Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY'
                ,'ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49'
                ,'DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh'
                ,'ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt'
                ,'DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL'
                ,'3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT')
and t.mint = 'So11111111111111111111111111111111111111111'
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(day)) from {{ this }})
{% endif %}
group by 1
order by 1 desc
