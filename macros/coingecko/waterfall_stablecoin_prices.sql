{% macro waterfall_stablecoin_prices(token_data_name, price_feed_name) %}
coalesce(
        {{price_feed_name}}.shifted_token_price_usd, 
        case 
            when {{token_data_name}}.coingecko_id = 'flex-usd' then (
                select shifted_token_price_usd
                from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
                where coingecko_id = 'flex-usd'
                qualify row_number() over (partition by coingecko_id order by date desc) = 1
            ) 
            when {{token_data_name}}.coingecko_id = 'anchored-coins-eur' then (
                select shifted_token_price_usd
                from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
                where coingecko_id = 'anchored-coins-eur'
                qualify row_number() over (partition by coingecko_id order by date desc) = 1
            ) 
            when {{token_data_name}}.coingecko_id = 'tether-eurt' then ({{ avg_l7d_coingecko_price('tether-eurt') }})
            when {{token_data_name}}.coingecko_id = 'stasis-eurs' then ({{ avg_l7d_coingecko_price('stasis-eurs') }})
            when {{token_data_name}}.coingecko_id = 'ageur' then ({{ avg_l7d_coingecko_price('ageur') }})
            when {{token_data_name}}.coingecko_id = 'euro-coin' then ({{ avg_l7d_coingecko_price('euro-coin') }})
            when {{token_data_name}}.coingecko_id = 'celo-euro' then ({{ avg_l7d_coingecko_price('celo-euro') }})
            when {{token_data_name}}.coingecko_id = 'celo-real-creal' then ({{ avg_l7d_coingecko_price('celo-real-creal') }})
            when {{token_data_name}}.coingecko_id = 'bilira' then ({{ avg_l7d_coingecko_price('bilira') }})
            when {{token_data_name}}.coingecko_id = 'brla-digital-brla' then ({{ avg_l7d_coingecko_price('brla-digital-brla') }})
            when {{token_data_name}}.coingecko_id = 'rupiah-token' then ({{ avg_l7d_coingecko_price('rupiah-token') }})
            when {{token_data_name}}.coingecko_id = 'idrx' then ({{ avg_l7d_coingecko_price('idrx') }})
            when {{token_data_name}}.coingecko_id = 'celo-kenyan-shilling' then ({{ avg_l7d_coingecko_price('celo-kenyan-shilling') }})
            else 1
        end
    )
{% endmacro %}