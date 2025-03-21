{{
    config(
        materialized='incremental',
        unique_key='day',
        snowflake_warehouse='JITO'
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
    , sum(t.amount * p.price) as tip_fees
    , sum(CASE WHEN t.block_timestamp < '2025-03-07'
        THEN t.amount * p.price * 0.05
        ELSE t.amount * p.price * 0.057 -- 2.7% to DAO + 3% to Jito
    END) as tip_revenue
    , sum(CASE WHEN t.block_timestamp < '2025-03-07'
        THEN t.amount * p.price
        ELSE t.amount * p.price * 0.943 -- 94% to validators + 0.3% to SOL/JTO vault operators
    END) as tip_supply_side_fees
    , count(*) as tip_txns
    , count(distinct t.tx_from) as tip_dau
FROM {{ source('SOLANA_FLIPSIDE', 'fact_transfers') }} t
LEFT JOIN prices p on p.date = date_trunc('day',t.block_timestamp)
WHERE tx_to IN ('96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5' -- all the tip payment accounts from: https://jito-foundation.gitbook.io/mev/mev-payment-and-distribution/on-chain-addresses#mainnet
                ,'HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe'
                ,'Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY'
                ,'ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49'
                ,'DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh'
                ,'ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt'
                ,'DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL'
                ,'3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT')
and t.mint = 'So11111111111111111111111111111111111111111'
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -6, max(day)) from {{ this }})
{% endif %}
group by 1
order by 1 desc
