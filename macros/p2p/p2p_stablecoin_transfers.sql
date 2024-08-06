{% macro p2p_stablecoin_transfers(chain) %}
with 
    stablecoin_transfers as (select * from {{ ref("fact_" ~ chain ~ "_stablecoin_transfers") }}),
    {% if chain in ("tron", "solana", "near") %}
         distinct_peer_address as (
            select address
            from {{ ref("dim_" ~ chain ~ "_eoa_addresses") }}
        ),
        stablecoin_transfers_with_prices as (
            select
                t1.block_timestamp,
                t1.block_number,
                t1.tx_hash,
                t1.index,
                t1.contract_address as token_address,
                t1.from_address,
                t1.to_address,
                t1.amount,
                coalesce(
                    fact_coingecko_token_date_adjusted_gold.shifted_token_price_usd * transfer_volume, 1 * transfer_volume
                ) as amount_usd
            from stablecoin_transfers t1
            join
                fact_{{ chain }}_stablecoin_contracts
                on lower(t1.contract_address)
                = lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
            left join
                fact_coingecko_token_date_adjusted_gold
                on lower(fact_{{ chain }}_stablecoin_contracts.coingecko_id)
                = lower(fact_coingecko_token_date_adjusted_gold.coingecko_id)
                and t1.date = fact_coingecko_token_date_adjusted_gold.date
            inner join distinct_peer_address t2 on lower(t1.to_address) = lower(t2.address)
            inner join distinct_peer_address t3 on lower(t1.from_address) = lower(t3.address)
            {% if chain == "solana" %}
                where block_timestamp::date > '2022-12-31' -- Prior to 2023, volumes data not high fidelity enough to report. Continuing to do analysis on this data. 
            {% endif %}
            {% if is_incremental() %} 
                {% if chain == "solana" %}
                    and 
                {% else %}
                    where 
                {% endif %}
                block_timestamp >= (
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
        stablecoin_transfers_with_prices as (
            select
                t1.block_timestamp,
                t1.block_number,
                t1.tx_hash,
                t1.index,
                t1.contract_address as token_address,
                t1.from_address,
                t1.to_address,
                t1.amount,
                coalesce(
                    fact_coingecko_token_date_adjusted_gold.shifted_token_price_usd * transfer_volume, 1 * transfer_volume
                ) as amount_usd
            from stablecoin_transfers t1
            join fact_{{ chain }}_stablecoin_contracts 
                on lower(t1.contract_address) = lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
            left join fact_coingecko_token_date_adjusted_gold 
                on lower(fact_{{ chain }}_stablecoin_contracts.coingecko_id) = lower(fact_coingecko_token_date_adjusted_gold.coingecko_id)
                and t1.date = fact_coingecko_token_date_adjusted_gold.date
            where not t1.to_address in (select contract_address from distinct_contracts)
                and not t1.from_address in (select contract_address from distinct_contracts)
            {% if is_incremental() %} 
                and block_timestamp >= (
                    select dateadd('day', -3, max(block_timestamp))
                    from {{ this }}
                )
            {% endif %}
        )
    {% endif %}
     , cex_contracts as (
        select address, app, sub_category from {{ ref("dim_contracts_gold")}} where chain = '{{ chain }}' and lower(sub_category) in ('cex', 'market maker')
    )
    , cex_filter as (
        select distinct tx_hash 
        from stablecoin_transfers_with_prices
        left join cex_contracts t1 on lower(from_address) = lower(t1.address)
        left join cex_contracts t2 on lower(to_address) = lower(t2.address)
        where t1.app = t2.app
            and lower(t1.sub_category) in ('cex', 'market maker') 
    )
    select
        t1.block_timestamp,
        t1.block_number,
        t1.tx_hash,
        t1.index,
        t1.token_address,
        t1.from_address,
        t1.to_address,
        t1.amount,
        t1.amount_usd
    from stablecoin_transfers_with_prices t1
    where from_address != to_address
        and from_address is not null and to_address is not null
        and lower(to_address) not in ('TMerfyf1KwvKeszfVoLH3PEJH52fC2DENq', '1nc1nerator11111111111111111111111111111111', 'system', '0x0000000000000000000000000000000000000000', 'EQAj-peZGPH-cC25EAv4Q-h8cBXszTmkch6ba6wXC8BM4xdo')
        and lower(from_address) not in ('TMerfyf1KwvKeszfVoLH3PEJH52fC2DENq', '1nc1nerator11111111111111111111111111111111', 'system', '0x0000000000000000000000000000000000000000', 'EQAj-peZGPH-cC25EAv4Q-h8cBXszTmkch6ba6wXC8BM4xdo')
        and lower(tx_hash) not in (select lower(tx_hash) from cex_filter)
    {% if chain == "solana" %}
        and block_timestamp::date > '2022-12-31' -- Prior to 2023, volumes data not high fidelity enough to report. Continuing to do analysis on this data. 
    {% endif %}
    {% if is_incremental() %} 
        and block_timestamp >= (
            select dateadd('day', -3, max(block_timestamp))
            from {{ this }}
        )
    {% endif %}

{% endmacro %}