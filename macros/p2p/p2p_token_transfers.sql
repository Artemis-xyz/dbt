{% macro p2p_token_transfers(chain) %}
    with 
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
                    amount as amount_precise,
                    token_address,
                    usd_amount as amount_usd
                from tron_allium.assets.trc20_token_transfers
            {% else %}
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
            {% endif %}
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
        {% if chain == "tron" %}
            and lower(to_address) != lower('TMerfyf1KwvKeszfVoLH3PEJH52fC2DENq')
            and lower(from_address) != lower('TMerfyf1KwvKeszfVoLH3PEJH52fC2DENq')
        {% else %}
            and lower(from_address) != lower('0x0000000000000000000000000000000000000000')
            and lower(to_address) != lower('0x0000000000000000000000000000000000000000')
        {% endif %}
    {% if is_incremental() %} 
        and block_timestamp >= (
            select dateadd('day', -3, max(block_timestamp))
            from {{ this }}
        )
    {% endif %}
{% endmacro %}
