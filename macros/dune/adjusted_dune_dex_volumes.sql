{% macro adjusted_dune_dex_volumes(chain) %}
    {% set coingecko_chain = 'avalanche' if chain == 'avalanche_c'
        else ('bsc' if chain == 'bnb'
        else ('polygon-zkevm' if chain == 'zkevm'
        else ('flare-network' if chain == 'flare'
        else chain))) %}

    WITH coingecko_latest_prices AS (
        {{ get_multiple_coingecko_price_with_latest(coingecko_chain) }}
    ), 

    partitioned_coingecko_prices AS (
        SELECT *, 
            ROW_NUMBER() OVER (PARTITION BY date, contract_address ORDER BY price DESC) AS rn
        FROM coingecko_latest_prices
    ),

    dex_trades_with_tx_count AS (
        SELECT *,
            COUNT(*) OVER (PARTITION BY block_date::date, tx_hash_hex, tx_from_hex) AS tx_count
        FROM {{ source("DUNE_DEX_VOLUMES", "trades") }}
        WHERE blockchain = '{{ chain }}'
    )

    SELECT 
        block_date::date AS date,
        SUM(COALESCE(amount_usd, 0)) AS daily_volume_adjusted
    FROM dex_trades_with_tx_count AS dex_trades

    LEFT JOIN partitioned_coingecko_prices AS token_bought_price
        ON dex_trades.block_date::date = token_bought_price.date
        AND LOWER(dex_trades.token_bought_address_hex) = LOWER(token_bought_price.contract_address)
        AND token_bought_price.rn = 1

    LEFT JOIN partitioned_coingecko_prices AS token_sold_price
        ON dex_trades.block_date::date = token_sold_price.date
        AND LOWER(dex_trades.token_sold_address_hex) = LOWER(token_sold_price.contract_address)
        AND token_sold_price.rn = 1

    WHERE
        (
            ((token_bought_price.price IS NULL OR token_sold_price.price IS NULL) AND amount_usd < 1000000)
            OR (
                (token_bought_price.price * token_bought_amount) / NULLIF(token_sold_price.price * token_sold_amount, 0)
                BETWEEN 0.5 AND 2.5
            )
        )
        AND tx_count < 100

    GROUP BY 1
{% endmacro %}
