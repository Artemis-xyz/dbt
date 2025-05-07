{% macro get_bananagun_metrics(chain) %}

WITH trades AS (
    SELECT *
    FROM {{ ref('fact_bananagun_' ~ chain ~ '_trades') }}
),
fees AS (
    SELECT *
    FROM {{ ref('fact_bananagun_' ~ chain ~ '_fees') }}
)

SELECT
    trades.block_timestamp::date as trade_date,
    SUM(trades.amount_in_usd) as "trading_volume",
    COUNT(DISTINCT trades.trader_address) as "dau",
    COUNT(DISTINCT trades.transaction_hash) as "daily_txns",
    SUM(COALESCE(fees.fee_usd, 0)) as "fees_usd"
FROM
    trades
    LEFT JOIN fees ON trades.transaction_hash = fees.transaction_hash
WHERE fees.fee_usd < 1e6
{% if is_incremental() %}
    AND trades.block_timestamp > (SELECT dateadd('day', -3, MAX(trade_date)) FROM {{ this }})
{% endif %}
GROUP BY
    trades.block_timestamp::date
ORDER BY
    trade_date DESC

{% endmacro %}
