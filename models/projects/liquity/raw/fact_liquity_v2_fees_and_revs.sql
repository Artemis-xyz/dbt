{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_v2_fees_and_revs'
    )
}}

-- original query at https://dune.com/queries/4462463/7465203

with 
    days AS (
        select
            date as day
        FROM
            pc_dbt_db.prod.dim_date_spine
        where
            date between '2025-01-01'
            and to_date(sysdate())
    ),
    eth_fee as(
        select
            block_timestamp::date as date,
            tx_hash,
            PC_DBT_DB.PROD.HEX_TO_INT(data)::number /1e18 as fee
        from
            ethereum_flipside.core.fact_event_logs
        where 1=1
            AND topics[0] = lower('0xc7e8309b9b14e7a8561ed352b9fd8733de32417fb7b6a69f5671f79e7bb29ddd')
)
    , add_price as (
        select
            ef.date,
            tx_hash,
            fee,
            p.price,
            fee * p.price as fee_usd
        from
            eth_fee ef
            left join ethereum_flipside.price.ez_prices_hourly p on ef.date = p.hour
            and p.symbol = 'WETH'
    )
    , interest_rewards as (
        select
            case
                when to_address = '0xcf46dab575c364a8b91bda147720ff4361f4627f' then 'wstETH'
                when to_address = '0xc4463b26be1a6064000558a84ef9b6a58abe4f7a' then 'rETH'
                when to_address = '0xf69eb8c0d95d4094c16686769460f678727393cf' then 'WETH'
                when to_address = '0x636deb767cd7d0f15ca4ab8ea9a9b26e98b426ac' then 'PIL'
            end as collateral_type,
            date_trunc('day', block_timestamp) as day,
            sum(amount) as bold_amount
        from
            ethereum_flipside.core.ez_token_transfers
        where
            to_address in (
                '0xcf46dab575c364a8b91bda147720ff4361f4627f',
                '0xc4463b26be1a6064000558a84ef9b6a58abe4f7a',
                '0xf69eb8c0d95d4094c16686769460f678727393cf',
                '0x636deb767cd7d0f15ca4ab8ea9a9b26e98b426ac'
            )
            and from_address = '0x0000000000000000000000000000000000000000'
            and contract_address = lower('0xb01dd87b29d187f3e3a4bf6cdaebfb97f3d9ab98')
        group by
            1,
            2
    ),
    all_interest as (
        select
            day,
            case
                when collateral_type in ('wstETH', 'rETH', 'WETH') then 'SP Yield'
                else 'PIL Yield'
            end as fee_type,
            sum(bold_amount) as fee
        from
            interest_rewards
        where
            collateral_type in ('wstETH', 'rETH', 'WETH', 'PIL')
        group by
            1,
            2
        union all
        select
            date,
            'Redemption Fees' as fee_type,
            sum(fee_usd) as fee
        from
            add_price
        group by
            1,
            2
    ),
    get_next_day as (
        select
            *,
            sum(fee) over (
                partition by fee_type
                order by
                    day asc
            ) as fee_total,
            lead(day, 1, current_timestamp) over (
                partition by fee_type
                order by
                    day asc
            ) as next_day
        from
            all_interest
    )
select
    day,
    'ethereum' as chain,
    'BOLD' as token,
    fee as revenue_native,
    fee as revenue_usd
from
    (
        select
            b.*,
            coalesce(c.fee, 0) as fee
        from
            (
                select
                    d.day,
                    c.fee_type,
                    c.fee_total
                from
                    get_next_day c
                    inner join days d on c.day <= d.day
                    and d.day < c.next_day
            ) b
            left join get_next_day c on b.day = c.day
            and b.fee_type = c.fee_type
    )
order by
    day desc