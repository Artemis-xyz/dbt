{{
    config(
        materialized="incremental",
        snowflake_warehouse="JITO",
        unique_key=["date", "mint"]
    )
}}

SELECT
    block_timestamp::date as date,
    mint,
    sum(amount * price) as withdraw_management_fees
FROM {{ source('SOLANA_FLIPSIDE', 'fact_transfers') }} t
LEFT JOIN solana_flipside.price.ez_prices_hourly p on p.token_address = t.mint and p.hour = t.block_timestamp::date
WHERE 1=1
    AND tx_to = '5eosrve6LktMZgVNszYzebgmmC7BjLK8NoWyRQtcmGTF'
    AND mint = 'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn'
    {% if is_incremental() %}
        AND block_timestamp > (select dateadd(day, -3, max(date)) from {{ this }})
    {% endif %}
    AND amount * price < 1e5
GROUP BY 1, 2
ORDER BY 1 desc