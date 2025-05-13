{% macro extract_dune_dex_volumes(chain) %}
    WITH coingecko_latest_prices AS (
        {{ get_multiple_coingecko_price_with_latest(chain) }}
    )

    SELECT 
        block_date::date AS date,
        sum(amount_usd) AS daily_volume
    FROM {{ source("DUNE_DEX_VOLUMES", "trades")}}
    LEFT JOIN coingecko_latest_prices AS token_bought_price
        ON block_date::date = token_bought_price.date
        AND lower(token_bought_address_hex) = lower(token_bought_price.contract_address)
    LEFT JOIN coingecko_latest_prices AS token_sold_price
        ON block_date::date = token_sold_price.date
        AND lower(token_sold_address_hex) = lower(token_sold_price.contract_address)
    WHERE blockchain = '{{ chain }}'
        AND token_sold_price.price IS NOT NULL
        AND token_bought_price.price IS NOT NULL 
        AND token_sold_price.price * token_sold_amount != 0
        AND token_bought_price.price * token_bought_amount != 0
        AND (token_bought_price.price * token_bought_amount) / (token_sold_price.price * token_sold_amount) > 0.2
        AND (token_bought_price.price * token_bought_amount) / (token_sold_price.price * token_sold_amount) < 4
    GROUP BY 1
{% endmacro %}
