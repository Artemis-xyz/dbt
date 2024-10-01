{% macro waterfall_stablecoin_prices(token_data_name, price_feed_name) %}
coalesce(
        {{price_feed_name}}.shifted_token_price_usd, 
        case 
            when {{token_data_name}}.coingecko_id = 'euro-coin' then ({{ avg_l7d_coingecko_price('euro-coin') }})
            when {{token_data_name}}.coingecko_id = 'celo-euro' then ({{ avg_l7d_coingecko_price('celo-euro') }})
            when {{token_data_name}}.coingecko_id = 'celo-real-creal' then ({{ avg_l7d_coingecko_price('celo-real-creal') }})
            when {{token_data_name}}.coingecko_id = 'celo-kenyan-shilling' then ({{ avg_l7d_coingecko_price('celo-kenyan-shilling') }})
            else 1
        end
    )
{% endmacro %}