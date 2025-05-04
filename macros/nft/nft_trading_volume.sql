{% macro nft_trading_volume(chain) %}
    {% if chain == "solana" %}
        select
            block_timestamp::date as date,
            '{{ chain }}' as chain,
            sum(coalesce(price_usd, 0)) nft_trading_volume
        from solana_flipside.nft.ez_nft_sales
        left join
            ({{ get_coingecko_price_with_latest("solana") }}) prices
            on block_timestamp::date = prices.date
        where program_id != 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN' -- bug in data where sales_amount is not the actual sales amount
        group by block_timestamp::date
        order by date desc
    {% elif chain == "flow" %}
        select
            block_timestamp::date as date,
            '{{ chain }}' as chain,
            sum(coalesce(t1.price * t2.price, 0)) nft_trading_volume
        from flow_flipside.nft.ez_nft_sales t1
        left join
            (
                select hour::date as date, token_address as token_contract, avg(price) as price
                from flow_flipside.price.ez_prices_hourly
                group by date, token_contract
            ) t2
            on block_timestamp::date = t2.date
            and lower(currency) = lower(token_contract)
        group by block_timestamp::date
        order by date desc
    {% else %}
        select
            '{{ chain }}' as chain,
            block_timestamp::date as date,
            sum(coalesce(price_usd, 0)) nft_trading_volume
        from {{ chain }}_flipside.nft.ez_nft_sales
        where block_timestamp is not null
        group by date
        order by date desc
    {% endif %}
{% endmacro %}
