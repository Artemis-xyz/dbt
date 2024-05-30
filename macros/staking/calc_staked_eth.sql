{% macro calc_staked_eth(table_name, is_restaking=false) %}
with
    temp as (
        select
            f.date,
            f.total_supply,
            f.total_supply * p.price as total_supply_usd,
            p.price
        from {{ ref(table_name) }}  f
        join ({{ get_coingecko_metrics("ethereum") }}) as p
            on f.date = p.date
        order by date desc
    )
select
    t.date,
    t.total_supply as {% if is_restaking %} num_restaked_eth {% else %} num_staked_eth {% endif %},
    t.total_supply_usd {% if is_restaking %} amount_restaked_usd {% else %} amount_staked_usd {% endif %},
    case
        when t.date = (select min(date) from {{ ref(table_name) }})
        then 0
        else t.total_supply - lag(t.total_supply, 1) over (order by t.date)
    end as {% if is_restaking %} num_restaked_eth_net_change {% else %} num_staked_eth_net_change {% endif %},
    case
        when t.date = (select min(date) from {{ ref(table_name) }})
        then 0
        else
            (t.total_supply - lag(t.total_supply, 1) over (order by t.date)) * (
                (
                    t.price
                    + lag(t.price, 1) over (order by t.date)
                )
                / 2
            )
    end as {% if is_restaking %} amount_restaked_usd_net_change {% else %} amount_staked_usd_net_change {% endif %}  
from temp t
order by date desc
{% endmacro %}
