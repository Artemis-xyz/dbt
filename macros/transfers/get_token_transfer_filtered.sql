{% macro get_token_transfer_filtered(chain, limit_number=1000, blacklist=('')) %}
With
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
    ),
    tokens as (
        select distinct token_in as token
        from {{ chain }}_flipside.defi.ez_dex_swaps 
        where lower(token_in) in (select lower(token_in) from dex_swap_liquidity_pairs)
        union
        select distinct token_out as token
        from {{ chain }}_flipside.defi.ez_dex_swaps 
        where lower(token_out) in (select lower(token_out) from dex_swap_liquidity_pairs)
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
    where 
        amount_usd is not null 
            and token_address in (select token from tokens) 
            {% if blacklist is string %} and lower(token_address) != lower('{{ blacklist }}')
            {% elif blacklist | length > 1 %} and token_address not in {{ blacklist }} --make sure you pass in lower
            {% endif %}
        and to_address != from_address 
        and lower(from_address) != lower('0x0000000000000000000000000000000000000000')
        and lower(to_address) != lower('0x0000000000000000000000000000000000000000')
        {% if is_incremental() %} 
            and block_timestamp >= (
                select dateadd('day', -3, max(block_timestamp))
                from {{ this }}
            )
        {% endif %}

{% endmacro %}
