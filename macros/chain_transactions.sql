{% macro chain_transactions(chain, token_address, bots_model) %}

    with
        new_contracts as (
            select distinct
                address,
                contract.name,
                contract.chain,
                contract.category,
                contract.sub_category,
                contract.app,
                contract.friendly_name
            from {{ ref("dim_contracts_gold") }} as contract
            where chain = {{ chain }}
        ),
        prices as (
            select date_trunc('day', hour) as price_date, avg(price) as price
            from ethereum_flipside.price.fact_hourly_token_prices
            where
            {% if var(chain) == "arbitrum" %}
                where
                    lower(t.from_address)
                    <> lower('0x00000000000000000000000000000000000a4b05')
                    and lower(to_address)
                    <> lower('0x00000000000000000000000000000000000a4b05')
            {% elif class == "base" %}
                where
                    lower(t.from_address)
                    <> lower('0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001')
            {% elif class == "blast" %}
                where
                    lower(t.from_address)
                    <> lower('0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001')
            {% elif class == "optimism" %}
                where
                    lower(t.from_address)
                    <> lower('0x420000000000000000000000000000000000000F')
                    and lower(to_address)
                    <> lower('0x420000000000000000000000000000000000000F')
            {% endif %}
                {% if is_incremental() %}
                    and hour
                    >= (select DATEADD('day', -3, max(block_timestamp)) from {{ this }})
                {% endif %}
            group by 1
        )
    select
        tx_hash,
        coalesce(to_address, t.from_address) as contract_address,
        block_timestamp,
        date_trunc('day', block_timestamp) raw_date,
        t.from_address,
        tx_fee,
        (tx_fee * price) gas_usd,
        {{ chain }} as chain,
        new_contracts.name,
        new_contracts.app,
        new_contracts.friendly_name,
        new_contracts.sub_category,
        case
            when new_contracts.category is not null
            then new_contracts.category
            when t.input_data = '0x'
            then 'EOA'
            else null
        end as category,
        bots.user_type
    from {{ chain }}_flipside.core.fact_transactions as t
    left join new_contracts on lower(t.to_address) = lower(new_contracts.address)
    left join prices on raw_date = prices.price_date
    left join prod.{{ bots_model }} as bots on t.from_address = bots.from_address
    {% if var(chain) == "arbitrum" %}
        where
            lower(t.from_address) <> lower('0x00000000000000000000000000000000000a4b05')
            and lower(to_address) <> lower('0x00000000000000000000000000000000000a4b05')
    {% elif class == "base" %}
        where
            lower(t.from_address) <> lower('0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001')
    {% elif class == "optimism" %}
        where
            lower(t.from_address) <> lower('0x420000000000000000000000000000000000000F')
            and lower(to_address) <> lower('0x420000000000000000000000000000000000000F')
    {% endif %}
{% endmacro %}
