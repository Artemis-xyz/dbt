{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="RAYDIUM",
    )
}}

with agg as (
    SELECT
        DATE(block_timestamp) as date,
        account_address,
        SUM(balance - pre_balance) AS difference
    FROM
        solana_flipside.core.fact_token_balances
    WHERE
        account_address in (
            'fArUAncZwVbMimiWv5cPUfFsapdLd8QMXZE4VXqFagR'
            , 'DmKR61BQk5zJTNCK9rrt8fM8HrDH6DSdE3Rt7sXKoAKb'
            , 'HoVhs7NAzcAaavr2tc2aaTynJ6kwhdfC2B2Z7EthKpeo'
            , '85WdjCsxksAoCo6NNZSU4rocjUHsJwXrzpHJScg8oVNZ'
            , 'HuBBhoS81jyHTKMbhz8B3iYa8HSNShnRwXRzPzmFFuFr'
            , '5unqG9sYX995czHCtHkkJvd2EaTE58jdvmjfz1nVvo5x'
            )
            AND mint = '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R'
            GROUP BY
                1, 2
            HAVING
                ABS(SUM(balance - pre_balance)) >= 0.1
            ORDER BY
                DATE(block_timestamp) desc
)
, mid as (
    SELECT
        date,
        sum(difference) as net_change,
        SUM(case when account_address = 'fArUAncZwVbMimiWv5cPUfFsapdLd8QMXZE4VXqFagR'
            then difference end) as gross_emissions,
        SUM(case when account_address <> 'fArUAncZwVbMimiWv5cPUfFsapdLd8QMXZE4VXqFagR'
            then difference end) as premine_unlocks
    FROM agg 
    group by 1
)
, date_spine as (
    SELECT
        ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from mid) and to_date(sysdate())
)
SELECT
    date_spine.date,
    coalesce(net_change, 0) as net_supply_change_native,
    coalesce(gross_emissions, 0) as gross_emissions_native,
    coalesce(premine_unlocks, 0) as premine_unlocks_native,
    sum(coalesce(net_change, 0)) over (order by date asc) as locked_supply_native,
    555000000 - locked_supply_native as circulating_supply_native -- Raydium max supply = 555M
FROM date_spine
left join mid using (date)
