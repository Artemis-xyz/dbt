{% macro rwa_balances(chain) %}
    -- This macro takes our balances table and forward fills the values for each address for each rwa

    {% set new_rwa_address = '' %}

with
    rwa_balances as (
        select 
            block_timestamp
            , t1.contract_address
            , symbol
            , address
            {% if chain in ('solana') %}
                , amount as rwa_supply_native
            {% elif chain in ('plume') %}
                , balance_raw / pow(10, num_decimals) as rwa_supply_native
            {% else %}
                , balance_token / pow(10, num_decimals) as rwa_supply_native
            {% endif %}

        {% if chain in ('plume') %}
            from {{ ref("fact_" ~ chain ~ "_address_balances")}} t1
        {% else %}
            from {{ ref("fact_" ~ chain ~ "_address_balances_by_token")}} t1
        {% endif %}
        inner join {{ ref("fact_" ~ chain ~ "_rwa_addresses")}} t2
            on lower(t1.contract_address) = lower(t2.contract_address)
        where block_timestamp < to_date(sysdate())
            {% if new_rwa_address != '' %}
                and lower(t1.contract_address) = lower('{{ new_rwa_address }}')
            {% endif %}
            {% if is_incremental() and new_rwa_address == '' %}
                    and block_timestamp > (select dateadd('day', -3, max(date)) from {{ this }}) 
            {% endif %}
    )
    {% if is_incremental() and new_rwa_address == '' %}
        --Get the most recent data in the existing table
        , stale_rwa_balances as (
            select 
                date as block_timestamp
                , t.contract_address
                , t.symbol
                , t.address
                , t.rwa_supply_native
            from {{ this }} t
            where date = (select dateadd('day', -3, max(date)) from {{ this }})
        )
    {% endif %}
    , heal_balance_table as (
        -- rwa_balances and stale_rwa_balances do not over lap
        -- rwa_balances select every row greater than the most recent date in the table
        -- stale_rwa_balances selects the most recent date in the table
        select
            block_timestamp
            , contract_address
            , symbol
            , address
            , rwa_supply_native
        from rwa_balances
        {% if is_incremental() and new_rwa_address == '' %}
            union
            select 
                block_timestamp
                , contract_address
                , symbol
                , address
                , rwa_supply_native
            from stale_rwa_balances
        {% endif %}
    ) 
    -- get the latest balance for each address for each date
    , balances as (
        select 
            block_timestamp::date as date
            , contract_address
            , symbol
            , address
            , rwa_supply_native
        from (
            select 
                block_timestamp
                , contract_address
                , symbol
                , address
                , rwa_supply_native
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
                rwa_supply_native, 
                LAST_VALUE(balances.rwa_supply_native ignore nulls) over (
                    partition by contract_address, address, symbol
                    order by date
                    rows between unbounded preceding and current row
                ) 
            )  as rwa_supply_native
        from date_range
        left join balances using (date, contract_address, symbol, address)
    )
    , rwa_balances_with_price as (
        select
            st.date
            , address
            , st.contract_address
            , st.symbol
            , coalesce( d.price,
                    case 
                        when c.coingecko_id in (
                                'blackrock-usd-institutional-digital-liquidity-fund', 
                                'franklin-onchain-u-s-government-money-fund',
                                'wisdomtree-government-money-market-digital-fund'
                                )
                            then 1  
                        when c.coingecko_id = 'hashnote-usyc'
                            then coalesce(h.rate, 1)
                        when c.coingecko_id = 'ousg'
                            then o.price
                        when c.coingecko_id = 'openeden-tbill'
                            then tbill.price
                        when st.contract_address = '0xe86845788d6e3e5c2393ade1a051ae617d974c09'
                            then d2.price
                                        end,
                    FIRST_VALUE(d.price IGNORE NULLS) OVER (PARTITION BY st.contract_address ORDER BY st.date ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW)
            ) as price_adj
            , rwa_supply_native
            , rwa_supply_native * price_adj as rwa_supply_usd
        from historical_supply_by_address_balances st
        left join {{ ref( "fact_" ~ chain ~ "_rwa_addresses") }} c
                on lower(st.contract_address) = lower(c.contract_address)
        left join (
            {{get_multiple_coingecko_price_with_latest(chain)}}
        ) d
            on lower(c.contract_address) = lower(d.contract_address)
            and st.date = d.date::date
        left join (
            {{get_multiple_coingecko_price_with_latest(chain)}}
        ) d2
            on lower(c.symbol) = lower(d2.symbol)
            and lower(c.contract_address) != lower(d2.contract_address)
            and st.date = d2.date::date
        left join {{ ref( "fact_hashnote_usyc_rate") }} h
            on st.date = h.date
            and st.symbol = 'USYC'
        left join {{ ref( "fact_ousg_prices") }} o
            on st.date = o.date
            and st.symbol = 'OUSG'
        left join (
            {{ get_coingecko_price_with_latest('openeden-tbill') }}
        ) tbill
            on st.date = tbill.date
            and st.symbol = 'TBILL'
    )
select
    date
    , address
    , contract_address
    , symbol
    , price_adj as price
    , rwa_supply_native
    , rwa_supply_usd
    , '{{ chain }}' as chain
    , date || '-' || address || '-' || contract_address as unique_id
from rwa_balances_with_price
where date < to_date(sysdate())
{% endmacro %}