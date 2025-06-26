{% macro stablecoin_balances(chain, new_stablecoin_address) %}
    -- This macro takes our balances table and forward fills the values for each address for each stablecoin

    -- Use this for a backfill, 
    -- It is important to backfill 6 months at a time otherwise the query will
    -- take > 4 hours on an XL to run
    -- Make sure to set to '' after backfill is complete

    {% set backfill_date = '' %}

    -- This is used to backfill a new stablecoin, make sure the run is incremental

with
    stablecoin_balances as (
        select 
            block_timestamp
            , t1.contract_address
            , symbol
            , address
    {% if chain in ('solana') %}
            , amount as stablecoin_supply_native
        from {{ ref("fact_" ~ chain ~ "_address_balances_by_token")}} t1
    {% elif chain in ('celo', 'base', 'sonic', 'tron', 'kaia', 'aptos', 'ripple') %}
            , balance_raw / pow(10, num_decimals) as stablecoin_supply_native
        from {{ ref("fact_"~chain~"_address_balances")}} t1
    {% else %}
            , balance_token / pow(10, num_decimals) as stablecoin_supply_native
        from {{ ref("fact_" ~ chain ~ "_address_balances_by_token")}} t1
    {% endif %}
    
        inner join {{ ref("fact_" ~ chain ~ "_stablecoin_contracts")}} t2
            on lower(t1.contract_address) = lower(t2.contract_address)
        where block_timestamp < to_date(sysdate())
            {% if chain == 'tron' %}
                and lower(address) != lower('t9yd14nj9j7xab4dbgeix9h8unkkhxuwwb') --Tron Burn Address
            {% elif chain == 'ton' %}
                and lower(address) != lower('EQAj-peZGPH-cC25EAv4Q-h8cBXszTmkch6ba6wXC8BM4xdo') --TON Burn Address
            {% endif %}
            {% if backfill_date != '' %}
                and block_timestamp < '{{ backfill_date }}'
            {% endif %}
            {% if new_stablecoin_address != '' %}
                and lower(t1.contract_address) = lower('{{ new_stablecoin_address }}')
            {% endif %}
            {% if is_incremental() and new_stablecoin_address == '' %}
                    and block_timestamp > (select dateadd('day', -3, max(date)) from {{ this }}) 
            {% endif %}
    )
    {% if is_incremental() and new_stablecoin_address == '' %}
        --Get the most recent data in the existing table
        , stale_stablecoin_balances as (
            select 
                date as block_timestamp
                , t.contract_address
                , t.symbol
                , t.address
                , t.stablecoin_supply_native
            from {{ this }} t
            where date = (select dateadd('day', -3, max(date)) from {{ this }})
        )
    {% endif %}
    , heal_balance_table as (
        -- stablecoin_balances and stale_stablecoin_balances do not over lap
        -- stablecoin balances select every row greater than the most recent date in the table
        -- stale_stablecoin_balances selects the most recent date in the table
        select
            block_timestamp
            , contract_address
            , symbol
            , address
            , stablecoin_supply_native
        from stablecoin_balances
        {% if is_incremental() and new_stablecoin_address == '' %}
            union
            select 
                block_timestamp
                , contract_address
                , symbol
                , address
                , stablecoin_supply_native
            from stale_stablecoin_balances
        {% endif %}
    ) 
    -- get the latest balance for each address for each date
    , balances as (
        select 
            block_timestamp::date as date
            , contract_address
            , symbol
            , address
            , stablecoin_supply_native
        from (
            select 
                block_timestamp
                , contract_address
                , symbol
                , address
                , stablecoin_supply_native
                , row_number() over (partition by block_timestamp::date, contract_address, address, symbol order by block_timestamp desc) AS rn
            from heal_balance_table
        )
        where rn = 1
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
    , historical_supply_by_address_balances as (
        select
            date
            , address
            , contract_address
            , symbol
            , coalesce(
                stablecoin_supply_native, 
                LAST_VALUE(balances.stablecoin_supply_native ignore nulls) over (
                    partition by contract_address, address, symbol
                    order by date
                    rows between unbounded preceding and current row
                ) 
            )  as stablecoin_supply_native
        from date_range
        left join balances using (date, contract_address, symbol, address)
    )
    , stablecoin_balances_with_price as (
        select
            st.date
            , address
            , st.contract_address
            , st.symbol
            , stablecoin_supply_native
            , stablecoin_supply_native * {{waterfall_stablecoin_prices('c', 'd')}} as stablecoin_supply
        from historical_supply_by_address_balances st
        left join {{ ref( "fact_" ~ chain ~ "_stablecoin_contracts") }} c
                on lower(st.contract_address) = lower(c.contract_address)
        left join {{ ref( "fact_coingecko_token_date_adjusted_gold") }} d
            on lower(c.coingecko_id) = lower(d.coingecko_id)
            and st.date = d.date::date
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
where date < to_date(sysdate())
{% endmacro %}