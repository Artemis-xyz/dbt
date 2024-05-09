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
            amount,
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
                    {% if is_incremental() %} 
                        where block_timestamp >= (
                            select dateadd('day', -3, max(block_timestamp))
                            from {{ this }}
                        )
                    {% endif %}
                    group by date, token_address
                ),
                dex_swap_liquidity_pairs as (
                    select 
                        t1.swap_from_mint as token_in, 
                        t1.swap_to_mint as token_out,
                        sum(swap_from_amount * t2.price) as amount_in_usd
                    from solana_flipside.defi.fact_swaps t1
                    left join token_prices t2 on t2.token_address = t1.swap_from_mint and t1.block_timestamp::date = t2.date
                    left join token_prices t3 on t3.token_address = t1.swap_to_mint and t1.block_timestamp::date = t3.date
                    where swap_to_amount = 0 and swap_from_amount = 0
                        and swap_from_amount * t2.price is null and swap_to_amount * t3.price is null 
                        and abs(
                            ln(coalesce(nullif(swap_from_amount * t2.price, 0), 1)) / ln(10)
                            - ln(coalesce(nullif(swap_to_amount * t3.price, 0), 1)) / ln(10)
                        )
                        < 1
                        and block_timestamp > '2022-12-31'
                        group by token_in, token_out
                        order by amount_in_usd desc
                        limit {{ limit_number }}
                ),
                tokens as (
                    select distinct swap_from_mint as token
                    from {{ chain }}_flipside.defi.fact_swaps 
                    where lower(swap_from_mint) not in (select lower(contract_address) from {{ref("fact_" ~ chain ~ "_stablecoin_contracts") }})
                        and lower(swap_from_mint) in (select lower(token_in) from dex_swap_liquidity_pairs)
                    union
                    select distinct swap_to_mint as token
                    from {{ chain }}_flipside.defi.fact_swaps 
                    where lower(swap_to_mint) not in (select lower(contract_address) from {{ref("fact_" ~ chain ~ "_stablecoin_contracts") }})
                        and lower(swap_to_mint) in (select lower(token_out) from dex_swap_liquidity_pairs)
                )
            {% elif chain == "near" %}
                 dex_swap_liquidity_pairs as (
                    select distinct
                        token_in_contract as token_in, 
                        token_out_contract as token_out,
                        sum(amount_in_usd) as amount_in_usd
                    from {{ chain }}_flipside.defi.ez_dex_swaps 
                    where (amount_in_usd is not null and amount_out_usd is not null) 
                        and (amount_in_usd <> 0 and amount_out_usd <> 0) and
                        abs(
                            ln(coalesce(nullif(amount_in_usd, 0), 1)) / ln(10)
                            - ln(coalesce(nullif(amount_out_usd, 0), 1)) / ln(10)
                        )
                        < 1
                    group by token_in, token_out
                    order by amount_in_usd desc
                    limit {{ limit_number }}
                ),
                tokens as (
                    select distinct token_in_contract as token
                    from {{ chain }}_flipside.defi.ez_dex_swaps 
                    where lower(token_in_contract) not in (select lower(contract_address) from {{ref("fact_" ~ chain ~ "_stablecoin_contracts") }})
                        and lower(token_in_contract) in (select lower(token_in) from dex_swap_liquidity_pairs)
                    union
                    select distinct token_out_contract as token
                    from {{ chain }}_flipside.defi.ez_dex_swaps 
                    where lower(token_out_contract) not in (select lower(contract_address) from {{ref("fact_" ~ chain ~ "_stablecoin_contracts") }})
                        and lower(token_out_contract) in (select lower(token_out) from dex_swap_liquidity_pairs)
                )
            {% else %}
                dex_swap_liquidity_pairs as (-- all pairs with non-zero liquidity
                    select distinct
                        token_in, 
                        token_out,
                        sum(amount_in_usd) as amount_in_usd
                    from {{ chain }}_flipside.defi.ez_dex_swaps 
                    where (amount_in_usd is not null and amount_out_usd is not null) and
                        (amount_in_usd <> 0 and amount_out_usd <> 0) and
                        abs(
                            ln(coalesce(nullif(amount_in_usd, 0), 1)) / ln(10)
                            - ln(coalesce(nullif(amount_out_usd, 0), 1)) / ln(10)
                        )
                        < 1
                    group by token_in, token_out
                    order by amount_in_usd desc
                    limit {{ limit_number }}
                ),
                tokens as (
                    select distinct token_in as token
                    from {{ chain }}_flipside.defi.ez_dex_swaps 
                    where lower(token_in) not in (select lower(contract_address) from {{ref("fact_" ~ chain ~ "_stablecoin_contracts") }})
                        and lower(token_in) in (select lower(token_in) from dex_swap_liquidity_pairs)
                    union
                    select distinct token_out as token
                    from {{ chain }}_flipside.defi.ez_dex_swaps 
                    where lower(token_out) not in (select lower(contract_address) from {{ref("fact_" ~ chain ~ "_stablecoin_contracts") }})
                        and lower(token_out) in (select lower(token_out) from dex_swap_liquidity_pairs)
                )
            {% endif %}
        select 
            block_timestamp,
            block_number,
            tx_hash,
            index,
            token_address,
            from_address,
            to_address,
            amount,
            amount_usd
        from {{ ref("fact_"~ chain ~"_p2p_token_transfers_silver")}} 
        where amount_usd is not null 
            and token_address in (select token from tokens) 
            {% if blacklist is string %} and lower(token_address) != lower('{{ blacklist }}')
            {% elif blacklist | length > 1 %} and token_address not in {{ blacklist }} --make sure you pass in lower
            {% endif %}
            {% if chain == "solana" %}
                and block_timestamp > '2022-12-31' -- Prior to 2023, volumes data not high fidelity enough to report. Continuing to do analysis on this data. 
            {% endif %}
            {% if is_incremental() %} 
                and block_timestamp >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )
            {% endif %}
    {% endif %}
{% endmacro %}