{% macro rwa_supply_by_date_and_chain(chain) %}
with rwa_balances as (
    select
        date
        , chain
        , symbol
        , avg(price) as price
        , sum(rwa_supply_native) as rwa_supply_native
        , sum(rwa_supply_usd) as rwa_supply_usd
    from {{ ref("fact_" ~ chain ~ "_rwa_balances") }}
    where rwa_supply_native >= 1e-9
    group by 1, 2, 3
)
select
    date
    , chain
    , symbol
    , price
    , rwa_supply_native - lag(rwa_supply_native) over (partition by symbol order by date) as net_rwa_supply_native_change
    , rwa_supply_usd - lag(rwa_supply_usd) over (partition by symbol order by date) as net_rwa_supply_usd_change
    , rwa_supply_native
    , rwa_supply_usd
from rwa_balances
{% endmacro %}
