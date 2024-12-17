{% macro get_single_address_historical_balance_by_token_and_chain(chain, address, start_date) %}
    with eod_balances as (
        select
            block_timestamp::date as date,
            user_address,
            contract_address,
            max_by(balance, block_timestamp) as eod_balance
        from
            {{ source('{{chain}}' | upper ~ '_FLIPSIDE', 'fact_token_balances') }}
        where 1=1
            and user_address = lower('{{address}}')
            and contract_address != '0x1a44e35d5451e0b78621a1b3e7a53dfaa306b1d0' -- token causing price issues
        GROUP BY 1,2,3
        
    )
    ,date_address_spine as (
        SELECT
            distinct
            ds.date,
            user_address,
            contract_address
        FROM
            {{ ref('dim_date_spine') }} ds
        CROSS JOIN eod_balances
        WHERE ds.date between '{{start_date}}' and to_date(sysdate())
    )
    , daily_balances as (
        SELECT
            das.date,
            das.user_address,
            das.contract_address,
            COALESCE(eod_balance,
                LAST_VALUE(
                    eod_balance
                ) IGNORE NULLS OVER(
                    partition by das.user_address, das.contract_address
                    ORDER BY das.date asc
                )) as eod_balance_ff 
        FROM date_address_spine das
        LEFT JOIN eod_balances e on e.date = das.date and e.contract_address = das.contract_address
    )
    select
        b.date,
        b.user_address,
        b.contract_address,
        sum(b.eod_balance_ff / pow(10, p.decimals)) as balance_native,
            sum(b.eod_balance_ff * p.price / pow(10, p.decimals)) as balance_usd
        from
            daily_balances b
        left join {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p ON p.hour = date_trunc('hour', b.date) and p.token_address = b.contract_address
        where 1=1
            and b.eod_balance_ff / pow(10, p.decimals) is not null
            and b.eod_balance_ff * p.price / pow(10, p.decimals) < pow(10,9)
        group by 1,2,3
        order by 1 desc, 5 desc
{% endmacro %}