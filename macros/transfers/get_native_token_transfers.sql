{% macro get_native_token_transfers(chain) %}
With
    {% if chain == "avalanche" %}
        prices as ({{ get_coingecko_price_with_latest("avalanche-2") }})
    {% elif chain == "polygon" %}
        prices as ({{ get_coingecko_price_with_latest("matic-network") }})
    {% elif chain == "tron" %}
        prices as ({{ get_coingecko_price_with_latest("tron") }})
    {% elif chain == "solana" %}
        prices as ({{ get_coingecko_price_with_latest("solana") }})
    {% elif chain == "near" %}
        prices as ({{ get_coingecko_price_with_latest("near") }})
    {% else %}
        prices as ({{ get_coingecko_price_with_latest("ethereum") }})
    {% endif %}
    select 
        block_timestamp,
        block_number,
        tx_hash,
        trace_index as index,
        from_address,
        to_address,
        amount_precise as amount,
        'native' as token_address,
        coalesce(t1.amount_precise * price, 0) as amount_usd
    from {{ chain }}_flipside.core.ez_native_transfers t1
    left join prices on prices.date = t1.block_timestamp::date
    where
        to_address != from_address
        and lower(from_address) != lower('0x0000000000000000000000000000000000000000')
        and lower(to_address) != lower('0x0000000000000000000000000000000000000000')
        {% if is_incremental() %} 
            and block_timestamp >= (
                select dateadd('day', -3, max(block_timestamp))
                from {{ this }}
            )
        {% endif %}
{% endmacro %}
