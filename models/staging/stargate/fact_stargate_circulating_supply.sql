{{ config(materialized="table", snowflake_warehouse="MEDIUM") }}

with treasury_historical as (
    -- Ethereum historical data
    select 
        date_trunc('day', block_timestamp) as date,
        'Ethereum' as chain,
        'Treasury' as wallet_type,
        address,
        balance_token / 1e18 as stg_balance
    from {{ ref("fact_ethereum_address_balances_by_token") }}
    where lower(address) = lower('0x65bb797c2B9830d891D87288F029ed8dACc19705')
    and lower(contract_address) = lower('0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6')
    
    UNION ALL
    
    -- Avalanche historical data
    select 
        date_trunc('day', block_timestamp) as date,
        'Avalanche' as chain,
        'Treasury' as wallet_type,
        address,
        balance_token / 1e18 as stg_balance
    from {{ ref("fact_avalanche_address_balances_by_token") }}
    where lower(address) = lower('0x2B065946d41ADf43BBc3BaF8118ae94Ed19D7A40')
    and lower(contract_address) = lower('0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590')
    
    UNION ALL
    
    -- BSC historical data
    select 
        date_trunc('day', block_timestamp) as date,
        'BSC' as chain,
        'Treasury' as wallet_type,
        address,
        balance_token / 1e18 as stg_balance
    from {{ ref("fact_bsc_address_balances_by_token") }}
    where lower(address) = lower('0x6e690075eedBC52244Dd4822D9F7887d4f27442F')
    and lower(contract_address) = lower('0xB0D502E938ed5f4df2E681fE6E419ff29631d62b')
    
    UNION ALL
    
    -- Polygon historical data
    select 
        date_trunc('day', block_timestamp) as date,
        'Polygon' as chain,
        'Treasury' as wallet_type,
        address,
        balance_token / 1e18 as stg_balance
    from {{ ref("fact_polygon_address_balances_by_token") }}
    where lower(address) = lower('0x47290DE56E71DC6f46C26e50776fe86cc8b21656')
    and lower(contract_address) = lower('0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590')
    
    UNION ALL
    
    -- Arbitrum historical data
    select 
        date_trunc('day', block_timestamp) as date,
        'Arbitrum' as chain,
        'Treasury' as wallet_type,
        address,
        balance_token / 1e18 as stg_balance
    from {{ ref("fact_arbitrum_address_balances_by_token") }}
    where lower(address) = lower('0x9CD50907aeb5D16F29Bddf7e1aBb10018Ee8717d')
    and lower(contract_address) = lower('0x6694340fc020c5E6B96567843da2df01b2CE1eb6')
    
    UNION ALL
    
    -- Optimism historical data
    select 
        date_trunc('day', block_timestamp) as date,
        'Optimism' as chain,
        'Treasury' as wallet_type,
        address,
        balance_token / 1e18 as stg_balance
    from {{ ref("fact_optimism_address_balances_by_token") }}
    where lower(address) = lower('0x392AC17A9028515a3bFA6CCe51F8b70306C6bd43')
    and lower(contract_address) = lower('0x296F55F8Fb28E498B858d0BcDA06D955B2Cb3f97')
    
    UNION ALL
    
    -- Base historical data
    select 
        date_trunc('day', block_timestamp) as date,
        'Base' as chain,
        'Treasury' as wallet_type,
        address,
        balance_raw / 1e18 as stg_balance
    from {{ ref("fact_base_address_balances") }}
    where lower(address) = lower('0x81EAb64E630C4a2E3E849268A6B64cb76D1C8109')
    and lower(contract_address) = lower('0xE3B53AF74a4BF62Ae5511055290838050bf764Df')
),
vesting_historical as (
    -- Ethereum vesting wallet
    select 
        date_trunc('day', block_timestamp) as date,
        'Ethereum' as chain,
        'Vesting' as wallet_type,
        min(address) as address,
        min_by(balance_token / 1e18, balance_token) as stg_balance
    from {{ ref("fact_ethereum_address_balances_by_token") }}
    where lower(address) = lower('0x8A27E7e98f62295018611DD681Ec47C7d9FF633A')
    and lower(contract_address) = lower('0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6')
    group by 1
),

all_wallet_historical as (
    select * from treasury_historical
    union all
    select * from vesting_historical
),
daily_wallet_balances as (
    select 
        date,
        chain,
        wallet_type,
        last_value(stg_balance) over (
            PARTITION BY date, chain, wallet_type 
            order by date
        ) as wallet_balance
    from all_wallet_historical
),
unique_daily_wallet_balances as (
    select 
        date,
        chain,
        wallet_type,
        wallet_balance,
        row_number() over (
            PARTITION BY date, chain, wallet_type 
            order by date
        ) as rn
    from daily_wallet_balances
),
date_range as (
    select 
        min(date) as min_date,
        current_date() as max_date
    from unique_daily_wallet_balances
),
date_chain_wallet_combinations as (
    select 
        ds.date,
        c.chain,
        w.wallet_type
    from {{ ref("dim_date_spine") }} ds
    cross join date_range dr
    cross join (
        select distinct chain from unique_daily_wallet_balances
    ) c
    cross join (
        select distinct wallet_type from unique_daily_wallet_balances
    ) w
    where ds.date between dr.min_date and dr.max_date
),
complete_daily_series as (
    select 
        dc.date,
        dc.chain,
        dc.wallet_type,
        udb.wallet_balance
    from date_chain_wallet_combinations dc
    left join unique_daily_wallet_balances udb 
        on dc.date = udb.date 
        and dc.chain = udb.chain
        and dc.wallet_type = udb.wallet_type
        and udb.rn = 1
),
backfilled_daily_series as (
    select 
        date,
        chain,
        wallet_type,
        coalesce(
            wallet_balance,
            last_value(wallet_balance ignore nulls) over (
                partition by chain, wallet_type
                order by date 
                rows between unbounded preceding and current row
            )
        ) as wallet_balance
    from complete_daily_series
),
daily_wallet_type_totals as (
    select 
        date,
        wallet_type,
        sum(coalesce(wallet_balance, 0)) as total_balance
    from backfilled_daily_series
    group by date, wallet_type
),
daily_wallet_summary as (
    select
        date,
        max(case when wallet_type = 'Treasury' then total_balance else 0 end) as treasury_balance,
        max(case when wallet_type = 'Vesting' then total_balance else 0 end) as vesting_balance
    from daily_wallet_type_totals
    group by date
),
historical_circulating_supply as (
    select 
        date,
        treasury_balance,
        vesting_balance,
        -- Using 1 billion as total supply by vesting balance
        1000000000 - (vesting_balance) AS circulating_supply
    from daily_wallet_summary
    order by date
),
treasury_chain_breakdown as (
    select
        date,
        max(case when chain = 'Ethereum' and wallet_type = 'Treasury' then wallet_balance else 0 end) as ethereum_balance,
        max(case when chain = 'Avalanche' and wallet_type = 'Treasury' then wallet_balance else 0 end) as avalanche_balance,
        max(case when chain = 'BSC' and wallet_type = 'Treasury' then wallet_balance else 0 end) as bsc_balance,
        max(case when chain = 'Polygon' and wallet_type = 'Treasury' then wallet_balance else 0 end) as polygon_balance,
        max(case when chain = 'Arbitrum' and wallet_type = 'Treasury' then wallet_balance else 0 end) as arbitrum_balance,
        max(case when chain = 'Optimism' and wallet_type = 'Treasury' then wallet_balance else 0 end) as optimism_balance,
        max(case when chain = 'Base' and wallet_type = 'Treasury' then wallet_balance else 0 end) as base_balance
    from backfilled_daily_series
    group by date
)
select 
    hcs.date,
    hcs.treasury_balance,
    hcs.vesting_balance,
    hcs.treasury_balance + hcs.vesting_balance AS total_locked_balance,
    hcs.circulating_supply,
    round((hcs.circulating_supply / 1000000000) * 100, 2) as percent_circulating,
    -- Chain breakdown
    cb.ethereum_balance,
    cb.avalanche_balance,
    cb.bsc_balance,
    cb.polygon_balance,
    cb.arbitrum_balance,
    cb.optimism_balance,
    cb.base_balance,
    -- Chain percentages of total treasury
    case when hcs.treasury_balance > 0 
         then round((cb.ethereum_balance / hcs.treasury_balance) * 100, 2) 
         else 0 end as ethereum_percent,
    case when hcs.treasury_balance > 0 
         then round((cb.avalanche_balance / hcs.treasury_balance) * 100, 2) 
         else 0 end as avalanche_percent,
    case when hcs.treasury_balance > 0 
         then round((cb.bsc_balance / hcs.treasury_balance) * 100, 2) 
         else 0 end as bsc_percent,
    case when hcs.treasury_balance > 0 
         then round((cb.polygon_balance / hcs.treasury_balance) * 100, 2) 
         else 0 end as polygon_percent,
    case when hcs.treasury_balance > 0 
         then round((cb.arbitrum_balance / hcs.treasury_balance) * 100, 2) 
         else 0 end as arbitrum_percent,
    case when hcs.treasury_balance > 0 
         then round((cb.optimism_balance / hcs.treasury_balance) * 100, 2) 
         else 0 end as optimism_percent,
    case when hcs.treasury_balance > 0 
         then round((cb.base_balance / hcs.treasury_balance) * 100, 2) 
         else 0 end as base_percent
from historical_circulating_supply hcs
join treasury_chain_breakdown cb on hcs.date = cb.date
order by hcs.date desc