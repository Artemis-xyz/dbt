{% macro p2p_token_transfers(chain) %}
with 
    {% if chain == "solana" %}
        token_prices as (
            select
                hour::date as date,
                token_address,
                avg(price) as price
            from solana_flipside.price.ez_prices_hourly
            group by date, token_address
        ),
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
                t1.amount,
                mint as token_address,
                coalesce(amount * price, 0) as amount_usd
            from solana_flipside.core.fact_transfers t1
            inner join solana_flipside.core.fact_blocks t2 using(block_id)
            inner join distinct_peer_address t3 on lower(to_address) = lower(t3.address)
            inner join distinct_peer_address t4 on lower(from_address) = lower(t4.address)
            left join token_prices t5 on t1.mint = t5.token_address and t2.block_timestamp::date = t5.date
            where mint <> 'So11111111111111111111111111111111111111112'
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
                coalesce(amount_raw_precise/pow(10, t5.decimals), 0) as amount,
                t1.contract_address as token_address,
                coalesce((amount_raw_precise/pow(10, t5.decimals)) * price, 0) as amount_usd
            from near_flipside.core.ez_token_transfers t1
            inner join distinct_peer_address t2 on lower(to_address) = lower(t2.address)
            inner join distinct_peer_address t3 on lower(from_address) = lower(t3.address)
            inner join (
                select 
                    hour::date as date,
                    token_address,
                    max(decimals) as decimals,
                    avg(price) as price
                from near_flipside.price.ez_prices_hourly 
                group by 1, 2
            ) t5 on t5.token_address = t1.contract_address and block_timestamp::date = t5.date
            where from_address != to_address and transfer_type = 'nep141'
                and from_address is not null and to_address is not null
                and from_address <> 'system' and to_address <> 'system'
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
                    amount,
                    token_address,
                    usd_amount as amount_usd
                from tron_allium.assets.trc20_token_transfers
                where to_address != from_address 
                    and lower(to_address) != lower('TMerfyf1KwvKeszfVoLH3PEJH52fC2DENq')
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
                    event_index as index,
                    from_address,
                    to_address,
                    amount_precise as amount,
                    contract_address as token_address,
                    amount_usd
                from {{ chain }}_flipside.core.ez_token_transfers
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
    select distinct
        t1.block_timestamp,
        t1.block_number,
        t1.tx_hash,
        t1.index,
        t1.token_address,
        t1.from_address,
        t1.to_address,
        t1.amount,
        t1.amount_usd
    from transfers t1
    where lower(token_address) not in (select lower(contract_address) from {{ ref("fact_" ~ chain ~ "_stablecoin_contracts")}})
    {% if is_incremental() %} 
        and block_timestamp >= (
            select dateadd('day', -3, max(block_timestamp))
            from {{ this }}
        )
    {% endif %}
    
{% endmacro %}
