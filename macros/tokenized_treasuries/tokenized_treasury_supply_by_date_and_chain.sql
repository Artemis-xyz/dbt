{% macro tokenized_treasury_supply_by_date_and_chain(chain) %}
with tokenized_treasury_balances as (
    select
        date
        , chain
        , symbol
        , sum(tokenized_treasury_supply_native) as tokenized_treasury_supply_native
        , sum(tokenized_treasury_supply) as tokenized_treasury_supply
    from {{ ref("fact_" ~ chain ~ "_tokenized_treasury_balances") }}
    group by 1, 2, 3
)
select
    date
    , chain
    , symbol
    , tokenized_treasury_supply_native - lag(tokenized_treasury_supply_native) over (partition by symbol order by date) as net_tokenized_treasury_supply_native_change
    , tokenized_treasury_supply - lag(tokenized_treasury_supply) over (partition by symbol order by date) as net_tokenized_treasury_supply_change
    , tokenized_treasury_supply_native
    , tokenized_treasury_supply
from tokenized_treasury_balances
{% endmacro %}
