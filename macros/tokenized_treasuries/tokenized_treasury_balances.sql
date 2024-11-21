{% macro tokenized_treasury_balances(chain) %}
    -- This macro takes our balances table and forward fills the values for each address for each tokenized treasury

    -- Use this for a backfill, 
    -- It is important to backfill 6 months at a time otherwise the query will
    -- take > 4 hours on an XL to run
    -- Make sure to set to '' after backfill is complete

    {% set new_tokenized_treasury_address = '' %}

with
    tokenized_treasury_balances as (
        select 
            block_timestamp
            , t1.contract_address
            , symbol
            , address
            {% if chain in ('solana') %}
                , amount as tokenized_treasury_supply_native
            {% else %}
                , balance_token / pow(10, num_decimals) as tokenized_treasury_supply_native
            {% endif %}
        from {{ ref("fact_" ~ chain ~ "_address_balances_by_token")}} t1
        inner join {{ ref("fact_" ~ chain ~ "_tokenized_treasury_addresses")}} t2
            on lower(t1.contract_address) = lower(t2.contract_address)
        where block_timestamp < to_date(sysdate())
            {% if new_tokenized_treasury_address != '' %}
                and lower(t1.contract_address) = lower('{{ new_tokenized_treasury_address }}')
            {% endif %}
            {% if is_incremental() and new_tokenized_treasury_address == '' %}
                    and block_timestamp > (select dateadd('day', -3, max(date)) from {{ this }}) 
            {% endif %}
    )
    {% if is_incremental() and new_tokenized_treasury_address == '' %}
        --Get the most recent data in the existing table
        , stale_tokenized_treasury_balances as (
            select 
                date as block_timestamp
                , t.contract_address
                , t.symbol
                , t.address
                , t.tokenized_treasury_supply_native
            from {{ this }} t
            where date = (select dateadd('day', -3, max(date)) from {{ this }})
        )
    {% endif %}
    , heal_balance_table as (
        -- tokenized_treasury_balances and stale_tokenized_treasury_balances do not over lap
        -- tokenized_treasury_balances select every row greater than the most recent date in the table
        -- stale_tokenized_treasury_balances selects the most recent date in the table
        select
            block_timestamp
            , contract_address
            , symbol
            , address
            , tokenized_treasury_supply_native
        from tokenized_treasury_balances
        {% if is_incremental() and new_tokenized_treasury_address == '' %}
            union
            select 
                block_timestamp
                , contract_address
                , symbol
                , address
                , tokenized_treasury_supply_native
            from stale_tokenized_treasury_balances
        {% endif %}
    ) 
    -- get the latest balance for each address for each date
    , balances as (
        select 
            block_timestamp::date as date
            , contract_address
            , symbol
            , address
            , tokenized_treasury_supply_native
        from (
            select 
                block_timestamp
                , contract_address
                , symbol
                , address
                , tokenized_treasury_supply_native
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
    )
    , historical_supply_by_address_balances as (
        select
            date
            , address
            , contract_address
            , symbol
            , coalesce(
                tokenized_treasury_supply_native, 
                LAST_VALUE(balances.tokenized_treasury_supply_native ignore nulls) over (
                    partition by contract_address, address, symbol
                    order by date
                    rows between unbounded preceding and current row
                ) 
            )  as tokenized_treasury_supply_native
        from date_range
        left join balances using (date, contract_address, symbol, address)
    )
    , tokenized_treasury_balances_with_price as (
        select
            st.date
            , address
            , st.contract_address
            , st.symbol
            , tokenized_treasury_supply_native
            , tokenized_treasury_supply_native * 
                coalesce( d.shifted_token_price_usd,
                    case when 
                        c.coingecko_id in ('blackrock-usd-institutional-digital-liquidity-fund', 'franklin-onchain-u-s-government-money-fund')
                    then 1
                end
            ) as tokenized_treasury_supply
        from historical_supply_by_address_balances st
        left join {{ ref( "fact_" ~ chain ~ "_tokenized_treasury_addresses") }} c
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
    , tokenized_treasury_supply_native
    , tokenized_treasury_supply
    , '{{ chain }}' as chain
    , date || '-' || address || '-' || contract_address as unique_id
from tokenized_treasury_balances_with_price
where date < to_date(sysdate())
{% endmacro %}