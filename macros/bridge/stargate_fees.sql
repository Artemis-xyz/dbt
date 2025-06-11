{% macro stargate_fees(chain, fees_contract_address, token_address) %} 

WITH dvn_fees AS (
    SELECT
        cast(block_timestamp as date) as date,
        tx_hash,
        contract_address,
        max_by(decoded_log:"fees"[0] / 1e18, date) as dvnFees
    FROM {{chain}}_flipside.core.ez_decoded_event_logs
    WHERE lower(contract_address) = lower('{{fees_contract_address}}') and decoded_log:"fees"[0] is not null
    GROUP BY 1, 2, 3

),
executor_fees AS (
    SELECT
        cast(block_timestamp as date) AS date,
        tx_hash,
        contract_address,
        max_by(decoded_log:"fee" / 1e18, date) as executorFees
    FROM {{chain}}_flipside.core.ez_decoded_event_logs
    WHERE lower(contract_address) = lower('{{fees_contract_address}}') 
    AND event_name = 'ExecutorFeePaid' 
    AND decoded_log:"fee" / 1e18 < 10 -- To remove outliers
    GROUP BY 1, 2, 3
),
-- '2024-06-20 spiked in volume because LayerZero (ZRO) token was launched
{% if chain == 'arbitrum' %}
daily_prices AS (
    SELECT
        date_trunc('day', hour) AS date,
        avg(price) AS avg_price
    FROM {{chain}}_flipside.price.ez_prices_hourly
    WHERE token_address = lower('{{token_address}}')
    GROUP BY 1
)
{% else %}
daily_prices AS (
    SELECT
        date_trunc('day', hour) AS date,
        avg(price) AS avg_price
    FROM {{chain}}_flipside.price.ez_prices_hourly
    WHERE is_native = true
    GROUP BY 1
)
{% endif %}
SELECT
    d.date,
    d.tx_hash,
    d.contract_address,
    d.dvnFees,
    e.executorFees,
    coalesce(d.dvnFees, 0) + coalesce(e.executorFees, 0) as fees_native,
    p.avg_price,
    (coalesce(d.dvnFees, 0) + coalesce(e.executorFees, 0)) * p.avg_price AS fees_usd,
    '{{chain}}' AS chain
FROM dvn_fees d
LEFT JOIN executor_fees e ON d.tx_hash = e.tx_hash
LEFT JOIN daily_prices p ON d.date = p.date
ORDER BY 1

{% endmacro %}