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
    {% else %}
        with
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
            {% if blacklist is string %} and lower(token_address) != '{{ blacklist }}'
            {% elif blacklist | length > 1 %} and token_address not in {{ blacklist }}
            {% endif %}
    {% endif %}
{% endmacro %}