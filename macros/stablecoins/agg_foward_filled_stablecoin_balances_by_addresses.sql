{% macro agg_foward_filled_stablecoin_balances_by_addresses(chain) %}

-- Use this for a backfill, 
-- It is important to backfill 6 months at a time otherwise the query will
-- take > 4 hours on an XL to run
-- Make sure to set to '' after backfill is complete

    {% set backfill_date = '' %}
with
    stablecoin_senders as (select from_address from {{ ref("fact_" ~ chain ~ "_stablecoin_transfers")}})
    , stablecoin_balances as (
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
        where lower(address) in (select lower(from_address) from stablecoin_senders) and block_timestamp < to_date(sysdate())
            {% if backfill_date != '' %}
                and block_timestamp < '{{ backfill_date }}'
            {% endif %}
            and block_timestamp < to_date(sysdate())
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
        where address not in (select distinct (premint_address) from {{ ref("fact_solana_stablecoin_premint_addresses")}}) 
        {% if is_incremental() %}
            and date > (select max(date) from {{ this }})
        {% endif %}
    )
    , daily_flows as (
        select
            date
            , sum(inflow) inflow
            , lower(contract_address) as contract_address
            , symbol
        from {{ref("fact_" ~ chain ~ "_stablecoin_transfers")}} 
        {% if backfill_date != '' %}
            where date < '{{ backfill_date }}'
        {% endif %}
        
        group by date, contract_address, symbol
        union all
        select
            dateadd(
                day, -1, (select min(trunc(date, 'day')) from {{ref("fact_" ~ chain ~ "_stablecoin_transfers")}})
            ) as date
            , sum(initial_supply) as inflow
            , lower(contract_address) as contract_address
            , symbol
        from {{ref("fact_" ~ chain ~ "_stablecoin_contracts")}}
        {% if backfill_date != '' %}
            where date < '{{ backfill_date }}'
        {% endif %}
        group by contract_address, symbol
    )
    , historical_supply_by_inflow_outflow as (
        select
            date
            , symbol
            , stablecoin_supply
            , contract_address
        from (
            select
                date
                , symbol
                , sum(inflow) over (
                    partition by contract_address, symbol order by date asc
                ) as stablecoin_supply
                , contract_address
            from daily_flows
        )
    )
    , total_historical_supply_by_address_balances as (
        select
            date
            , contract_address
            , symbol
            , sum(stablecoin_supply) as stablecoin_supply
        from historical_supply_by_address_balances
        group by date, contract_address, symbol
    )
 
    select
        date
        , address
        , contract_address
        , symbol
        , stablecoin_supply
    from historical_supply_by_address_balances
    union
    -- this is a hacky way to fix the issue with the balances table and get the total supply for that day
    select 
        date
        , '0x00000000000000000000000000000DEADARTEMIS' as address
        , contract_address
        , symbol
        , historical_supply_by_inflow_outflow.stablecoin_supply - total_historical_supply_by_address_balances.stablecoin_supply as stablecoin_supply
    from total_historical_supply_by_address_balances
    left join historical_supply_by_inflow_outflow using (date, contract_address, symbol)
    
{% endmacro %}

    
