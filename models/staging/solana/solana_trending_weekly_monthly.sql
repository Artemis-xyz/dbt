{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE_XLG") }}

with
    solana_contracts as (
        select address, name, artemis_application_id as namespace, friendly_name, artemis_category_id AS category
        from {{ ref("dim_all_addresses_labeled_gold") }}
        where chain = 'solana'
    ),
    prices as (
        select date as price_date, shifted_token_price_usd as price
        from fact_coingecko_token_date_adjusted_gold
        where coingecko_id = 'solana'
        union
        select date as price_date, token_current_price as price
        from fact_coingecko_token_realtime_data
        where token_id = 'solana'
    ),
    chain_transactions as (
        select
            tx_id as tx_hash,
            max(block_timestamp) as block_timestamp,
            any_value(signers) as signers,
            min_by(value:"programId"::string, index) as program_id,
            max(fee) as fee,
            min(index) index,
            max(succeeded) as succeeded,
            any_value(post_token_balances) as post_token_balances
        from solana_flipside.core.fact_transactions, lateral flatten(instructions)
        where
            value:"programId"::string not in (
                'ComputeBudget111111111111111111111111111111',
                'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
                'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr',
                'Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo'
            )
            and block_timestamp >= dateadd('day', -60, current_date)
        group by tx_id
    ),
    last_2_month as (
        select
            t.signers as signers,
            program_id,
            date_trunc('day', block_timestamp) date,
            fee / pow(10, 9) tx_fee,
            price,
            succeeded,
            coalesce(token.name, solana_contracts.name) as name,
            coalesce(token.namespace, solana_contracts.namespace) as namespace,
            coalesce(
                token.friendly_name, solana_contracts.friendly_name
            ) as friendly_name,
            case
                when program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                then post_token_balances[0]:"mint"::string
                else null
            end as token_address,
            case
                when program_id = '11111111111111111111111111111111'
                then 'EOA'
                when token.category is not null
                then token.category
                when program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                then 'Token'
                when solana_contracts.category is not null
                then solana_contracts.category
                else null
            end as category
        from chain_transactions as t
        left join
            solana_contracts on lower(t.program_id) = lower(solana_contracts.address)
        left join
            solana_contracts as token on lower(token_address) = lower(token.address)
        left join prices on date_trunc('day', block_timestamp) = prices.price_date
    ),
    last_week as (
        select
            coalesce(token_address, program_id) as to_address,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then tx_fee * price else 0 end) gas_usd,
            max(name) name,
            max(namespace) namespace,
            max(friendly_name) friendly_name,
            max(category) category
        from last_2_month, lateral flatten(input => signers)
        where to_address is not null and date >= dateadd(day, -7, current_date)
        group by to_address
    ),
    two_week as (
        select
            coalesce(token_address, program_id) as to_address,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then tx_fee * price else 0 end) gas_usd
        from last_2_month, lateral flatten(input => signers)
        where
            to_address is not null
            and date < dateadd(day, -7, current_date)
            and date >= dateadd(day, -14, current_date)
        group by to_address
    ),
    trending_week as (
        select
            last_week.to_address,
            last_week.txns,
            last_week.gas,
            last_week.gas_usd,
            last_week.dau,
            two_week.txns prev_txns,
            two_week.gas prev_gas,
            two_week.gas_usd prev_gas_usd,
            two_week.dau prev_dau,
            last_week.name,
            last_week.namespace,
            last_week.friendly_name,
            last_week.category,
            'weekly' as granularity
        from last_week
        left join two_week on lower(last_week.to_address) = lower(two_week.to_address)
    ),
    last_month as (
        select
            coalesce(token_address, program_id) as to_address,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then tx_fee * price else 0 end) gas_usd,
            max(name) name,
            max(namespace) namespace,
            max(friendly_name) friendly_name,
            max(category) category
        from last_2_month, lateral flatten(input => signers)
        where to_address is not null and date >= dateadd(day, -30, current_date)
        group by to_address
    ),
    two_month as (
        select
            coalesce(token_address, program_id) as to_address,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then tx_fee * price else 0 end) gas_usd
        from last_2_month, lateral flatten(input => signers)
        where
            to_address is not null
            and date < dateadd(day, -30, current_date)
            and date >= dateadd(day, -60, current_date)
        group by to_address
    ),
    trending_month as (
        select
            last_month.to_address,
            last_month.txns,
            last_month.gas,
            last_month.gas_usd gas_usd,
            last_month.dau,
            two_month.txns prev_txns,
            two_month.gas prev_gas,
            two_month.gas_usd prev_gas_usd,
            two_month.dau prev_dau,
            last_month.name,
            last_month.namespace,
            last_month.friendly_name,
            last_month.category,
            'monthly' as granularity
        from last_month
        left join
            two_month on lower(last_month.to_address) = lower(two_month.to_address)
    )
select *
from trending_week
union
select *
from trending_month
