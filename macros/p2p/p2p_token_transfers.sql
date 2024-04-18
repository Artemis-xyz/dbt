{% macro p2p_token_transfers(chain) %}
    with 
        distinct_peer_address as (
            select address
            from {{ ref("dim_" ~ chain ~ "_eoa_addresses") }}
        ), 
        transfers as (
            select 
                block_timestamp,
                block_number,
                tx_hash,
                event_index as index,
                from_address,
                to_address,
                raw_amount_precise,
                amount_precise,
                contract_address as token_address,
                amount_usd
            from {{ chain }}_flipside.core.ez_token_transfers
            {% if is_incremental() %} 
                where block_timestamp >= (
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
        t1.token_address,
        t1.from_address,
        t1.to_address,
        t1.raw_amount_precise,
        t1.amount_precise,
        t1.amount_usd
    from transfers t1
    inner join distinct_peer_address t2 on lower(t1.to_address) = lower(t2.address)
    inner join distinct_peer_address t3 on lower(t1.from_address) = lower(t3.address)
    where to_address != from_address 
        and lower(from_address) != lower('0x0000000000000000000000000000000000000000')
        and lower(to_address) != lower('0x0000000000000000000000000000000000000000')
{% endmacro %}
