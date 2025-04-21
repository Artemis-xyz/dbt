{{ config(
    materialized= "incremental",
    unique_key="date",
    snowflake_warehouse="METAPLEX"
) }}

WITH 
buybacks AS (
    select 
       date_trunc('day', block_timestamp) as date
       , max(amount) as amount
       , max(mint) as token_address
    from solana_flipside.core.fact_transfers 
    where tx_to = 'E7Hzc1cQwx5BgJa8hJGVuDF2G2f2penLrhiKU6nU53gK' and tx_from = 'BBcPaj5v95nFFbXfgTebYyJDSY5HBCpARuRCLynVWimp'
    and mint = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
    {% if is_incremental() %}
    and block_timestamp >= (select dateadd('day', -3, max(date)) from {{ this }})
    {% else %}
    and block_timestamp >= '2024-06-26'
    {% endif %}
    GROUP BY date
)
, prices AS (
    {{ get_coingecko_price_with_latest('metaplex') }}
)
SELECT
    b.date,
    b.amount as buyback_native,
    b.amount * p.price AS buyback
FROM
    buybacks b
LEFT JOIN
    prices p
    ON b.date = p.date
ORDER BY
    b.date DESC
