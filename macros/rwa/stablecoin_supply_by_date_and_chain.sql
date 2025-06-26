{% macro stablecoin_supply_by_date_and_chain(chain) %}
with stablecoin_balances as (
    select
        date
        , chain
        , symbol
        , avg(price) as price
        , sum(stablecoin_supply_native) as stablecoin_supply_native
        , sum(stablecoin_supply_usd) as stablecoin_supply_usd
    from {{ ref("fact_" ~ chain ~ "_stablecoin_balances") }}
    where stablecoin_supply_native >= 1e-9
    group by 1, 2, 3
)
select
    date
    , chain
    , symbol
    , price
    , stablecoin_supply_native - lag(stablecoin_supply_native) over (partition by symbol order by date) as net_stablecoin_supply_native_change
    , stablecoin_supply_usd - lag(stablecoin_supply_usd) over (partition by symbol order by date) as net_stablecoin_supply_usd_change
    , stablecoin_supply_native
    , stablecoin_supply_usd
from stablecoin_balances
{% endmacro %}
