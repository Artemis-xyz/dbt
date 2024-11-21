{% macro rwa_supply_by_date_and_chain(chain) %}
with rwa_balances as (
    select
        date
        , chain
        , symbol
        , sum(rwa_supply_native) as rwa_supply_native
        , sum(rwa_supply) as rwa_supply
    from {{ ref("fact_" ~ chain ~ "_rwa_balances") }}
    group by 1, 2, 3
)
select
    date
    , chain
    , symbol
    , rwa_supply_native - lag(rwa_supply_native) over (partition by symbol order by date) as net_rwa_supply_native_change
    , rwa_supply - lag(rwa_supply) over (partition by symbol order by date) as net_rwa_supply_change
    , rwa_supply_native
    , rwa_supply
from rwa_balances
{% endmacro %}
