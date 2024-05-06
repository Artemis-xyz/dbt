{% macro p2p_native_transfers(chain) %}
    with 
        {% if chain == "avalanche" %}
            prices as ({{ get_coingecko_price_with_latest("avalanche-2") }}),
        {% elif chain == "polygon" %}
            prices as ({{ get_coingecko_price_with_latest("matic-network") }}),
        {% elif chain == "tron" %}
            prices as ({{ get_coingecko_price_with_latest("tron") }}),
        {% elif chain == "solana" %}
            prices as ({{ get_coingecko_price_with_latest("solana") }}),
        {% elif chain == "near" %}
            prices as ({{ get_coingecko_price_with_latest("near") }}),
        {% else %}
            prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
        {% endif %}
        distinct_peer_address as (
            select address
            from {{ ref("dim_" ~ chain ~ "_eoa_addresses") }}
        ), 
        transfers as (
            {% if chain == "tron" %}
                select 
                    block_timestamp,
                    block_number,
                    transaction_hash as tx_hash,
                    unique_id as index,
                    from_address,
                    to_address,
                    raw_amount as raw_amount_precise,
                    amount as amount_precise
                from tron_allium.assets.trx_token_transfers
                where to_address != from_address 
                    and lower(to_address) != lower('TMerfyf1KwvKeszfVoLH3PEJH52fC2DENq') -- Burn address of tron
                    and lower(from_address) != lower('TMerfyf1KwvKeszfVoLH3PEJH52fC2DENq')
            {% elif chain == "solana" %}
                select 
                    t2.block_timestamp,
                    t1.block_id as block_number,
                    t1.tx_id as tx_hash,
                    t1.index,
                    t1.tx_from as from_address,
                    t1.tx_to as to_address,
                    null as raw_amount_precise,
                    t1.amount as amount_precise
                from solana_flipside.core.fact_transfers t1
                inner join solana_flipside.core.fact_blocks t2 using(block_id)
                where mint = 'So11111111111111111111111111111111111111112'
                    and t1.tx_from != t1.tx_to
                    and lower(t1.tx_from) != lower('1nc1nerator11111111111111111111111111111111') -- Burn address of solana
                    and lower(t1.tx_to) != lower('1nc1nerator11111111111111111111111111111111')
            {% elif chain == "near" %}
                select 
                    block_timestamp,
                    block_id as block_number,
                    tx_hash,
                    fact_token_transfers_id as index,
                    from_address,
                    to_address,
                    amount_raw_precise as raw_amount_precise,
                    amount_raw_precise / 1E24 as amount_precise
                from near_flipside.core.ez_token_transfers
                where from_address != to_address and transfer_type = 'native'
                    and from_address is not null and to_address is not null
            {% else %}
                select 
                    block_timestamp,
                    block_number,
                    tx_hash,
                    trace_index as index,
                    from_address,
                    to_address,
                    amount_precise_raw as raw_amount_precise,
                    amount_precise
                from {{ chain }}_flipside.core.ez_native_transfers
                where to_address != from_address
                    and lower(from_address) != lower('0x0000000000000000000000000000000000000000')
                    and lower(to_address) != lower('0x0000000000000000000000000000000000000000')

            {% endif %}
            {% if is_incremental() %} 
                and block_timestamp >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )
            {% endif %}
        )
    select 
        t1.block_timestamp,
        t1.block_number,
        t1.tx_hash,
        t1.index,
        t1.from_address,
        t1.to_address,
        t1.raw_amount_precise,
        t1.amount_precise,
        coalesce(t1.amount_precise * price, 0) as amount_usd
    from transfers t1
    inner join distinct_peer_address t2 on lower(t1.to_address) = lower(t2.address)
    inner join distinct_peer_address t3 on lower(t1.from_address) = lower(t3.address)
    left join prices on prices.date = t1.block_timestamp::date
    {% if is_incremental() %} 
        where block_timestamp >= (
            select dateadd('day', -3, max(block_timestamp))
            from {{ this }}
        )
    {% endif %}
{% endmacro %}