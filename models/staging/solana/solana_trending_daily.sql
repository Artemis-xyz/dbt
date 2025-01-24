{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE_LG") }}

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
            and block_timestamp >= dateadd('day', -2, current_date)
        group by tx_id
    ),
    last_2_day as (
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
    last_day as (
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
        from last_2_day, lateral flatten(input => signers)
        where to_address is not null and date >= dateadd(day, -1, current_date)
        group by to_address
    ),
    two_days as (
        select
            coalesce(token_address, program_id) as to_address,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then tx_fee * price else 0 end) gas_usd
        from last_2_day, lateral flatten(input => signers)
        where
            program_id is not null
            and date < dateadd(day, -1, current_date)
            and date >= dateadd(day, -2, current_date)
        group by to_address
    )
select
    last_day.to_address,
    last_day.txns txns,
    last_day.gas gas,
    last_day.gas_usd gas_usd,
    last_day.dau dau,
    two_days.txns prev_txns,
    two_days.gas prev_gas,
    two_days.gas_usd prev_gas_usd,
    two_days.dau prev_dau,
    last_day.name,
    last_day.namespace,
    last_day.friendly_name,
    last_day.category,
    'daily' as granularity
from last_day
left join two_days on lower(last_day.to_address) = lower(two_days.to_address)
