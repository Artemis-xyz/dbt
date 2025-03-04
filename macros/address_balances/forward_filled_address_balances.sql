{% macro forward_filled_address_balances(chain, artemis_application_id, type) %}

with
    tagged_addresses as (
        select
            address
            , artemis_application_id
            , type
        from {{ ref("dim_all_addresses_labeled_gold") }}
        where artemis_application_id = '{{ artemis_application_id }}'
            and type = '{{ type }}'
            and chain = '{{ chain }}'
    )
    , token_metadata as (
        select
            contract_address
            , decimals
        from {{ ref("dim_coingecko_token_map") }}
        where chain = '{{ chain }}'

    )
    -- TODO: Remove once we have updated balances data
    , old_balances as (
        select
            address
            , case 
                when contract_address = 'native_token' and '{{chain}}' = 'ethereum' then 'eip155:1:native' 
                when contract_address = 'native_token' and '{{chain}}' = 'arbitrum' then 'eip155:42161:native' 
                when contract_address = 'native_token' and '{{chain}}' = 'base' then 'eip155:8453:native' 
                when contract_address = 'native_token' and '{{chain}}' = 'optimism' then 'eip155:10:native' 
                else contract_address 
            end as contract_address
            , block_timestamp
            , balance_token as balance_native
        from {{ ref("fact_" ~ chain ~ "_address_balances_by_token") }}
        where lower(address) in (select lower(address) from tagged_addresses)
    )
    , address_balances as (
        select
            ab.address
            , ab.contract_address
            , block_timestamp
            , balance_native
            , case
                when right(ab.contract_address, 6) = 'native' then balance_native
                else balance_native / pow(10, decimals) 
            end as balance_adjusted
        from old_balances ab
        inner join tagged_addresses
            on lower(ab.address) = lower(tagged_addresses.address)
        left join token_metadata
            on lower(ab.contract_address) = lower(token_metadata.contract_address)
        where block_timestamp < to_date(sysdate())
        {% if is_incremental() %}
            and block_timestamp > (select dateadd('day', -3, max(date)) from {{ this }}) 
        {% endif %}
    )
    {% if is_incremental() %}
        --Get the most recent data in the existing table
        , stale_balances as (
            select 
                date as block_timestamp
                , t.contract_address
                , t.address
                , t.balance_native
                , t.balance_adjusted
            from {{ this }} t
            where date = (select dateadd('day', -3, max(date)) from {{ this }})
        )
    {% endif %}
    , heal_balance_table as (
        -- address_balances and stale_address_balances do not over lap
        -- address_balances select every row greater than the most recent date in the table
        -- stale_address_balances selects the most recent date in the table
        select
            block_timestamp
            , contract_address
            , address
            , balance_native
            , balance_adjusted
        from address_balances
        {% if is_incremental() %}
            union
            select 
                block_timestamp
                , contract_address
                , address
                , balance_native
                , balance_adjusted
            from stale_balances
        {% endif %}
    ) 
    , balances as (
        select 
            block_timestamp::date as date
            , contract_address
            , address
            , balance_native
            , balance_adjusted
        from (
            select 
                block_timestamp
                , contract_address
                , address
                , balance_native
                , balance_adjusted
                , row_number() over (partition by block_timestamp::date, contract_address, address order by block_timestamp desc) AS rn
            from heal_balance_table
        )
        where rn = 1
    )
    , date_range as (
        select 
            min(block_timestamp)::date as date
            , contract_address
            , address
        from heal_balance_table
        group by contract_address, address
        
        union all   
        
        select
            dateadd(day, 1, date) as date
            , contract_address
            , address
        from date_range
        where date < dateadd(day, -1, to_date(sysdate()))
    )
    , historical_supply_by_address_balances as (
        select
            date
            , address
            , contract_address
            , coalesce(
                balance_native, 
                LAST_VALUE(balances.balance_native ignore nulls) over (
                    partition by contract_address, address
                    order by date
                    rows between unbounded preceding and current row
                ) 
            )  as balance_native
            , coalesce(
                balance_adjusted, 
                LAST_VALUE(balances.balance_adjusted ignore nulls) over (
                    partition by contract_address, address
                    order by date
                    rows between unbounded preceding and current row
                ) 
            )  as balance_adjusted
        from date_range
        left join balances using (date, contract_address, address)
    )
    , prices as ({{ get_multiple_coingecko_price_with_latest(chain) }} )
    , address_balances_with_prices as (
        select
            date
            , contract_address
            , address
            , price
            , balance_native
            , balance_adjusted
            , balance_adjusted * price as balance_usd
            , '{{ artemis_application_id }}' as artemis_application_id
            , '{{ type }}' as type
            , '{{ chain }}' as chain
            , date || '-' || address || '-' || contract_address as unique_id
        from historical_supply_by_address_balances
        left join prices using (date, contract_address)
    )
select 
    date
    , contract_address
    , address
    , balance_native
    , balance_adjusted
    , price
    , balance_usd
    , artemis_application_id
    , type
    , chain
    , unique_id
from address_balances_with_prices
{% endmacro %}