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
        
        {% if chain == "solana" %}
            distinct_peer_address as (
                select address
                from {{ ref("dim_" ~ chain ~ "_eoa_addresses") }}
            ),
            transfers as (
                select 
                    t2.block_timestamp,
                    t1.block_id as block_number,
                    t1.tx_id as tx_hash,
                    t1.index,
                    t1.tx_from as from_address,
                    t1.tx_to as to_address,
                    t1.amount
                from solana_flipside.core.fact_transfers t1
                inner join solana_flipside.core.fact_blocks t2 using(block_id)
                inner join distinct_peer_address t3 on lower(t1.tx_to) = lower(t3.address)
                inner join distinct_peer_address t4 on lower(t1.tx_from) = lower(t4.address)
                where mint = 'So11111111111111111111111111111111111111112'
                    and t1.tx_from != t1.tx_to
                    and lower(t1.tx_from) != lower('1nc1nerator11111111111111111111111111111111') -- Burn address of solana
                    and lower(t1.tx_to) != lower('1nc1nerator11111111111111111111111111111111')
                    {% if is_incremental() %} 
                        and block_timestamp >= (
                            select dateadd('day', -3, max(block_timestamp))
                            from {{ this }}
                        )       
                    {% endif %}
            )
        {% elif chain == "near" %}
            distinct_peer_address as (
                select address
                from {{ ref("dim_" ~ chain ~ "_eoa_addresses") }}
            ), 
            transfers as (
                select 
                    block_timestamp,
                    block_id as block_number,
                    tx_hash,
                    ez_token_transfers_id as index,
                    from_address,
                    to_address,
                    amount_raw_precise / 1E24 as amount
                from near_flipside.core.ez_token_transfers t1
                inner join distinct_peer_address t2 on lower(t1.to_address) = lower(t2.address)
                inner join distinct_peer_address t3 on lower(t1.from_address) = lower(t3.address)
                where from_address != to_address and transfer_type = 'native'
                    and from_address is not null and to_address is not null
                    {% if is_incremental() %} 
                        and block_timestamp >= (
                            select dateadd('day', -3, max(block_timestamp))
                            from {{ this }}
                        )       
                    {% endif %}
            )
        {% else %}
            distinct_contracts as (
                select contract_address
                from {{ ref("dim_" ~ chain ~ "_contract_addresses") }}
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
                        amount
                    from tron_allium.assets.trx_token_transfers t1
                    where to_address != from_address 
                        and lower(to_address) != lower('TMerfyf1KwvKeszfVoLH3PEJH52fC2DENq') -- Burn address of tron
                        and lower(from_address) != lower('TMerfyf1KwvKeszfVoLH3PEJH52fC2DENq')
                        and not lower(to_address) in (select lower(contract_address) from distinct_contracts)
                        and not lower(from_address) in (select lower(contract_address) from distinct_contracts)
                        {% if is_incremental() %} 
                            and block_timestamp >= (
                                select dateadd('day', -3, max(block_timestamp))
                                from {{ this }}
                            )       
                        {% endif %}
                {% else %}
                    select 
                        block_timestamp,
                        block_number,
                        tx_hash,
                        trace_index as index,
                        from_address,
                        to_address,
                        amount_precise as amount
                    from {{ chain }}_flipside.core.ez_native_transfers t1
                    where not to_address in (select contract_address from distinct_contracts)
                        and not from_address in (select contract_address from distinct_contracts)
                        and to_address != from_address
                        and lower(from_address) != lower('0x0000000000000000000000000000000000000000')
                        and lower(to_address) != lower('0x0000000000000000000000000000000000000000')
                        {% if is_incremental() %} 
                            and block_timestamp >= (
                                select dateadd('day', -3, max(block_timestamp))
                                from {{ this }}
                            )
                        {% endif %}
                {% endif %}
            )
        {% endif %}
    select 
        t1.block_timestamp,
        t1.block_number,
        t1.tx_hash,
        t1.index,
        t1.from_address,
        t1.to_address,
        t1.amount,
        coalesce(t1.amount * price, 0) as amount_usd
    from transfers t1
    left join prices on prices.date = t1.block_timestamp::date
    {% if is_incremental() %} 
        where block_timestamp >= (
            select dateadd('day', -3, max(block_timestamp))
            from {{ this }}
        )
    {% endif %}
    {% if chain == "near" %}
        qualify row_number() over (partition by tx_hash,index order by block_timestamp desc) = 1
    {% endif %}
{% endmacro %}