{% macro extract_dune_dex_volumes(chain) %}
    {% set coingecko_chain = 'avalanche' if chain == 'avalanche_c'
        else ('bsc' if chain == 'bnb'
        else ('polygon-zkevm' if chain == 'zkevm'
        else chain)) %}
    
    WITH coingecko_latest_prices AS (
        {{ get_multiple_coingecko_price_with_latest(coingecko_chain) }}
    ), 

    partitioned_coingecko_prices AS (
        SELECT *, 
            ROW_NUMBER() OVER (PARTITION BY date, contract_address ORDER BY price DESC) AS rn
        FROM coingecko_latest_prices
    )

    SELECT 
        block_date::date AS date,
        sum(amount_usd) AS daily_volume
    FROM {{ source("DUNE_DEX_VOLUMES", "trades")}}
    LEFT JOIN partitioned_coingecko_prices AS token_bought_price
        ON block_date::date = token_bought_price.date
        AND lower(token_bought_address_hex) = lower(token_bought_price.contract_address)
        AND token_bought_price.rn = 1
    LEFT JOIN partitioned_coingecko_prices AS token_sold_price
        ON block_date::date = token_sold_price.date
        AND lower(token_sold_address_hex) = lower(token_sold_price.contract_address)
        AND token_sold_price.rn = 1
    WHERE blockchain = '{{ chain }}'
        AND token_sold_price.price IS NOT NULL
        AND token_bought_price.price IS NOT NULL 
        AND token_sold_price.price * token_sold_amount != 0
        AND token_bought_price.price * token_bought_amount != 0
        AND (token_bought_price.price * token_bought_amount) / (token_sold_price.price * token_sold_amount) > 0.5
        AND (token_bought_price.price * token_bought_amount) / (token_sold_price.price * token_sold_amount) < 2.5
    GROUP BY 1
{% endmacro %}
