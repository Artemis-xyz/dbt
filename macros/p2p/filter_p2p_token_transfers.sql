{% macro filter_p2p_token_transfers(chain, limit_number=1000, blacklist=('')) %}
    {% if chain == "tron" %}
        select 
            block_timestamp,
            block_number,
            tx_hash,
            index,
            token_address,
            from_address,
            to_address,
            raw_amount_precise,
            amount_precise,
            amount_usd
        from {{ ref("fact_"~ chain ~"_p2p_token_transfers_silver")}} 
        where amount_usd is not null 
            {% if blacklist is string %} and token_address != '{{ blacklist }}'
            {% elif blacklist | length > 1 %} and token_address not in {{ blacklist }}
            {% endif %}
        {% if is_incremental() %} 
            and block_timestamp >= (
                select dateadd('day', -3, max(block_timestamp))
                from {{ this }}
            )
        {% endif %}
    {% else %}
        with
            {% if chain == "solana" %}
                token_prices as (
                    select
                        recorded_hour::date as date,
                        token_address,
                        avg(close) as price
                    from solana_flipside.price.ez_token_prices_hourly
                    group by date, token_address
                ),
                dex_swap_liquidity as (
                    select 
                        sum(swap_from_amount * t2.price) as daily_volume,
                        t1.swap_from_mint as token_in, 
                        t1.swap_to_mint as token_out
                    from solana_flipside.defi.fact_swaps t1
                    left join token_prices t2 on t2.token_address = t1.swap_from_mint and t1.block_timestamp::date = t2.date
                    left join token_prices t3 on t3.token_address = t1.swap_to_mint and t1.block_timestamp::date = t3.date
                    where swap_to_amount <> 0 and swap_from_amount <> 0
                        and swap_from_amount * t2.price is not null and swap_to_amount * t3.price is not null 
                        and coalesce(swap_from_amount * t2.price, 0) > 0 and coalesce(swap_to_amount * t3.price, 0) > 0 and
                        abs(
                            ln(coalesce(nullif(swap_from_amount * t2.price, 0), 1)) / ln(10)
                            - ln(coalesce(nullif(swap_to_amount * t3.price, 0), 1)) / ln(10)
                        )
                        < 1
                        {% if is_incremental() %} -- largest volume over the last 10 days
                            and block_timestamp >= (
                                select dateadd('day', -10, max(block_timestamp))
                                from {{ this }}
                            )
                        {% endif %}
                    group by token_in, token_out
                    order by daily_volume desc
                    limit {{ limit_number }}-- choose top pairs depending on the chain   
                ),
            {% else %}
                dex_swap_liquidity as (
                    select 
                        sum(amount_in_usd) as daily_volume,
                        token_in, 
                        token_out
                    from {{ chain }}_flipside.defi.ez_dex_swaps 
                    where amount_in_usd is not null and amount_out_usd is not null and
                        abs(
                            ln(coalesce(nullif(amount_in_usd, 0), 1)) / ln(10)
                            - ln(coalesce(nullif(amount_out_usd, 0), 1)) / ln(10)
                        )
                        < 1
                    group by token_in, token_out
                    order by daily_volume desc
                    limit {{ limit_number }}-- choose top pairs depending on the chain   
                ),
            {% endif %}
        tokens as (
            select distinct token_in as token
            from dex_swap_liquidity
            union
            select distinct token_out as token
            from dex_swap_liquidity
        )
        select 
            block_timestamp,
            block_number,
            tx_hash,
            index,
            token_address,
            from_address,
            to_address,
            raw_amount_precise,
            amount_precise,
            amount_usd
        from {{ ref("fact_"~ chain ~"_p2p_token_transfers_silver")}} 
        where amount_usd is not null 
            and token_address in (select token from tokens) 
            {% if blacklist is string %} and lower(token_address) != lower('{{ blacklist }}')
            {% elif blacklist | length > 1 %} and token_address not in {{ blacklist }} --make sure you pass in lower
            {% endif %}
            {% if is_incremental() %} 
                and block_timestamp >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )
            {% endif %}
    {% endif %}
{% endmacro %}