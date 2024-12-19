{% macro get_single_address_historical_balance_by_token_and_chain(chain, address, start_date, blacklist=('')) %}
    with eod_balances as (
        select
            block_timestamp::date as date,
            address,
            contract_address,
            max_by(balance_token, block_timestamp) as eod_balance
        from
            {{ ref('fact_' ~ chain ~ '_address_balances_by_token') }}
        where 1=1
            and address = lower('{{address}}')
            {% if blacklist is string %} and lower(contract_address) != lower('{{ blacklist }}')
            {% elif blacklist | length > 1 %} and contract_address not in {{ blacklist }} --make sure you pass in lower
            {% endif %}
        GROUP BY 1,2,3
        
    )
    ,date_address_spine as (
        SELECT
            distinct
            ds.date,
            address,
            contract_address
        FROM
            {{ ref('dim_date_spine') }} ds
        CROSS JOIN eod_balances
        WHERE ds.date between '{{start_date}}' and to_date(sysdate())
    )
    , daily_balances as (
        SELECT
            das.date,
            das.address,
            das.contract_address,
            COALESCE(eod_balance,
                LAST_VALUE(
                    eod_balance
                ) IGNORE NULLS OVER(
                    partition by das.address, das.contract_address
                    ORDER BY das.date asc
                )) as eod_balance_ff 
        FROM date_address_spine das
        LEFT JOIN eod_balances e on e.date = das.date and e.contract_address = das.contract_address
    )
    select
        b.date,
        '{{chain}}' as chain,
        b.address,
        b.contract_address,
        p.symbol,
        sum(b.eod_balance_ff / pow(10, p.decimals)) as balance_native,
        sum(b.eod_balance_ff * p.price / pow(10, p.decimals)) as balance_usd
    from
        daily_balances b
    left join {{ source(chain | upper ~ '_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p ON p.hour = date_trunc('hour', b.date) and p.token_address = b.contract_address
    where 1=1
        and b.eod_balance_ff / pow(10, p.decimals) is not null
        and b.eod_balance_ff * p.price / pow(10, p.decimals) < pow(10,9)
    group by 1,2,3,4,5
    having balance_usd > 0
    order by 1 desc, 5 desc
{% endmacro %}