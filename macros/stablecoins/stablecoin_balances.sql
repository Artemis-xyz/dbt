{% macro stablecoin_balances(chain) %}
    -- This macro takes our balances table and forward fills the values for each address for each stablecoin

    -- Use this for a backfill, 
    -- It is important to backfill 6 months at a time otherwise the query will
    -- take > 4 hours on an XL to run
    -- Make sure to set to '' after backfill is complete

    {% set backfill_date = '2021-01-01' %}
with
    stablecoin_balances as (
        select 
            block_timestamp
            , lower(t1.contract_address) as contract_address
            , symbol
            , lower(address) as address
            {% if chain in ('solana') %}
                , amount_unadj / pow(10, num_decimals) as stablecoin_supply
            {% else %}
                , balance_token / pow(10, num_decimals) as stablecoin_supply
            {% endif %}
        from {{ ref("fact_" ~ chain ~ "_address_balances_by_token")}} t1
        inner join {{ ref("fact_" ~ chain ~ "_stablecoin_contracts")}} t2
            on lower(t1.contract_address) = lower(t2.contract_address)
        where block_timestamp < to_date(sysdate())
            {% if chain == 'tron' %}
                and lower(address) != lower('t9yd14nj9j7xab4dbgeix9h8unkkhxuwwb') --Tron Burn Address
                and stablecoin_supply > 0
            {% endif %}
            {% if backfill_date != '' %}
                and block_timestamp < '{{ backfill_date }}'
            {% endif %}
        {% if is_incremental() %}
                and block_timestamp >= (select dateadd('day', -1, max(date)) from {{ this }})
        {% endif %}
    )
    {% if is_incremental() %}
        , stale_stablecoin_balances as (
            select 
                date as block_timestamp
                , t.contract_address
                , t.symbol
                , t.address
                , t.stablecoin_supply
            from {{this}} t
            left join (
                select distinct address, contract_address
                from stablecoin_balances
            ) sb on t.address = sb.address and t.contract_address = sb.contract_address
            where date >= (select dateadd('day', -1, max(date)) from {{ this }})
            and sb.address is null and sb.contract_address is null
        )
    {% endif %}
    , heal_balance_table as (
        select
            block_timestamp
            , contract_address
            , symbol
            , address
            , stablecoin_supply
        from stablecoin_balances
        {% if is_incremental() %}
            union
            select 
                block_timestamp
                , contract_address
                , symbol
                , address
                , stablecoin_supply
            from stale_stablecoin_balances
        {% endif %}
    ) 
    , date_range as (
        select 
            min(block_timestamp)::date as date
            , contract_address
            , symbol
            , address
        from heal_balance_table
        group by contract_address, address, symbol
        
        union all   
        
        select
            dateadd(day, 1, date) as date
            , contract_address
            , symbol
            , address
        from date_range
        where date < dateadd(day, -1, to_date(sysdate()))
        {% if backfill_date != '' %}
            and date < dateadd(day, -1, '{{ backfill_date }}')
        {% endif %}
    )
    , balances as (
        select 
            block_timestamp::date as date
            , contract_address
            , symbol
            , address
            , stablecoin_supply
        from (
            select 
                block_timestamp
                , contract_address
                , symbol
                , address
                , stablecoin_supply
                , row_number() over (partition by block_timestamp::date, contract_address, address, symbol order by block_timestamp desc) AS rn
            from heal_balance_table
        )
        where rn = 1
    )
    , historical_supply_by_address_balances as (
        select
            date
            , address
            , contract_address
            , symbol
            , coalesce(
                stablecoin_supply, 
                LAST_VALUE(balances.stablecoin_supply ignore nulls) over (
                    partition by contract_address, address, symbol
                    order by date
                    rows between unbounded preceding and current row
                ) 
            )  as stablecoin_supply
        from date_range
        left join balances using (date, contract_address, symbol, address)
        {% if is_incremental() %}
                where date >= (select dateadd('day', -1, max(date)) from {{ this }})
        {% endif %}
    )
    , stablecoin_balances_with_price as (
        select
            st.date
            , address
            , st.contract_address
            , st.symbol
            , stablecoin_supply as stablecoin_supply_native
            , stablecoin_supply * coalesce(
                d.token_current_price, 1
            ) as stablecoin_supply
        from historical_supply_by_address_balances st
        left join {{ ref( "fact_" ~ chain ~ "_stablecoin_contracts") }} c
                on lower(st.contract_address) = lower(c.contract_address)
        left join {{ ref( "fact_coingecko_token_realtime_data") }} d
            on lower(c.coingecko_id) = lower(d.token_id)
            and st.date = d.date::date
        {% if is_incremental() %}
                where st.date >= (select dateadd('day', -1, max(date)) from {{ this }})
        {% endif %}
    )
select
    date
    , address
    , contract_address
    , symbol
    , stablecoin_supply_native
    , stablecoin_supply
    , '{{ chain }}' as chain
    , date || '-' || address || '-' || contract_address as unique_id
from stablecoin_balances_with_price

{% endmacro %}