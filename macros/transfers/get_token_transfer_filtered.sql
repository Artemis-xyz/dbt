{% macro get_token_transfer_filtered(chain, limit_number=1000, blacklist=(''), whitelist=(''), include_full_history="FALSE" ) %}
With
    {% if chain == "solana" %}
        dex_swap_liquidity_pairs as (
            select 
                t1.swap_from_mint as token_in, 
                t1.swap_to_mint as token_out,
                sum(swap_from_amount_usd) as amount_in_usd
            from solana_flipside.defi.ez_dex_swaps t1
            where (swap_from_amount_usd is not null and swap_to_amount_usd is not null) 
                and (swap_from_amount_usd > 0 and swap_to_amount_usd > 0)
                and abs(
                    ln(coalesce(nullif(swap_from_amount_usd, 0), 1)) / ln(10)
                    - ln(coalesce(nullif(swap_to_amount_usd, 0), 1)) / ln(10)
                )
                < 1
                group by token_in, token_out
                order by amount_in_usd desc
                limit {{ limit_number }}
        )
        , tokens as (
            select distinct swap_from_mint as token
            from solana_flipside.defi.ez_dex_swaps
            where lower(swap_from_mint) in (select lower(token_in) from dex_swap_liquidity_pairs)
            union
            select distinct swap_to_mint as token
            from solana_flipside.defi.ez_dex_swaps
            where lower(swap_to_mint) in (select lower(token_out) from dex_swap_liquidity_pairs)
            {% if whitelist is string %}
                union 
                select '{{ whitelist }}' as token
            {% elif whitelist | length > 1 %}
                union
                select distinct token_address as token
                from values {{whitelist}} as t(token_address)
            {% endif %}
        )
        , token_prices as (
            select
                hour::date as date,
                token_address,
                avg(price) as price
            from solana_flipside.price.ez_prices_hourly
            group by date, token_address
        ) 
        select 
            t2.block_timestamp,
            t1.block_id as block_number,
            t1.tx_id as tx_hash,
            t1.index,
            t1.tx_from as from_address,
            t1.tx_to as to_address,
            t1.amount,
            mint as token_address,
            coalesce(amount * price, 0) as amount_usd
        from solana_flipside.core.fact_transfers t1
        inner join solana_flipside.core.fact_blocks t2 using(block_id)
        left join token_prices t5 on t1.mint = t5.token_address and t2.block_timestamp::date = t5.date
        where mint <> 'So11111111111111111111111111111111111111112'
            {% if include_full_history=="FALSE" %}
                and t1.tx_from != t1.tx_to
                and lower(t1.tx_from) != lower('1nc1nerator11111111111111111111111111111111') -- Burn address of solana
                and lower(t1.tx_to) != lower('1nc1nerator11111111111111111111111111111111')
            {% endif %}
            {% if is_incremental() %} 
                and block_timestamp >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )
            {% endif %}
    {% else %}
        dex_swap_liquidity_pairs as (-- all pairs with non-zero liquidity
            select distinct
                token_in, 
                token_out,
                sum(amount_in_usd) as amount_in_usd
            from {{ chain }}_flipside.defi.ez_dex_swaps 
            where (amount_in_usd is not null and amount_out_usd is not null) and
                (amount_in_usd > 0 and amount_out_usd > 0) and
                abs(
                    ln(coalesce(nullif(amount_in_usd, 0), 1)) / ln(10)
                    - ln(coalesce(nullif(amount_out_usd, 0), 1)) / ln(10)
                )
                < 1
            group by token_in, token_out
            order by amount_in_usd desc
            limit {{ limit_number }}
        )
        , tokens as (
            select distinct token_in as token
            from {{ chain }}_flipside.defi.ez_dex_swaps 
            where lower(token_in) in (select lower(token_in) from dex_swap_liquidity_pairs)
            union
            select distinct token_out as token
            from {{ chain }}_flipside.defi.ez_dex_swaps 
            where lower(token_out) in (select lower(token_out) from dex_swap_liquidity_pairs)
            {% if whitelist is string %}
                union 
                select '{{ whitelist }}' as token
            {% elif whitelist | length > 1 %}
                union
                select distinct token_address as token
                from values {{whitelist}} as t(token_address)
            {% endif %}
            
        )
        select 
            block_timestamp,
            block_number,
            tx_hash,
            event_index as index,
            from_address,
            to_address,
            amount_precise as amount,
            contract_address as token_address,
            amount_usd
        from {{ chain }}_flipside.core.ez_token_transfers
        where token_address in (select token from tokens) 
            {% if blacklist is string %} and lower(token_address) != lower('{{ blacklist }}')
            {% elif blacklist | length > 1 %} and token_address not in {{ blacklist }} --make sure you pass in lower
            {% endif %}
            {% if include_full_history=="FALSE" %}
                and amount_usd is not null 
                and to_address != from_address 
                and lower(from_address) != lower('0x0000000000000000000000000000000000000000')
                and lower(to_address) != lower('0x0000000000000000000000000000000000000000')
            {% endif %}
            {% if is_incremental() %} 
                and block_timestamp >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )
            {% endif %}
    {% endif %}
{% endmacro %}
