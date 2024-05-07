{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BAM_TRANSACTION_XLG",
    )
}}

with
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
            // Chunking required here for backfills
            {% if is_incremental() %}
                and block_timestamp
                >= (select dateadd('day', -5, max(block_timestamp)) from {{ this }})
        {% else %}
            -- Making code not compile on purpose. Full refresh of entire history
            -- takes too long, doing last month will wipe out backfill
            -- TODO: Figure out a workaround.
            where
                block_timestamp
                >= (select dateadd('month', -1, max(block_timestamp)) from {{ this }})
        {% endif %}
        group by tx_id
    ),
    app_contracts as (
        select distinct
            address,
            contract.name,
            contract.chain,
            contract.category,
            contract.sub_category,
            contract.app,
            contract.friendly_name
        from pc_dbt_db.prod.dim_contracts_gold as contract
        where chain = 'solana'
    ),
    token_contracts as (
        select address, name, chain, category
        from {{ ref("dim_flipside_contracts") }}
        where
            chain = 'solana'
            and category in ('NFT', 'Token')
            and sub_category in ('nf_token_contract', 'token_contract')
    ),
    prices as (
        select date as date, shifted_token_price_usd as price
        from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
        where coingecko_id = 'solana'
        union
        select dateadd('day', -1, date) as date, token_current_price as price
        from pc_dbt_db.prod.fact_coingecko_token_realtime_data
        where token_id = 'solana'
    ),
    collapsed_prices as (select date, max(price) as price from prices group by date),
    balances as (
        select address, date, balance_usd, native_token_balance, stablecoin_balance
        from {{ ref("fact_solana_daily_balances") }}
        where
            {% if is_incremental() %}
                date >= (select dateadd('day', -5, max(raw_date)) from {{ this }})
        {% else %}
            -- Making code not compile on purpose. Full refresh of entire history
            -- takes too long, doing last month will wipe out backfill
            -- TODO: Figure out a workaround.
            where date >= (select dateadd('month', -1, max(raw_date)) from {{ this }})
        {% endif %}
    ),
    tagged_transactions as (
        select
            tx_hash,
            date_trunc('day', block_timestamp) as raw_date,
            block_timestamp,
            signers,
            program_id,
            'solana' as chain,
            fee / 1e9 as tx_fee,
            (fee / 1e9) * price as gas_usd,
            succeeded,
            case
                when program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                then post_token_balances[0]:"mint"::string
                else null
            end as token_address,
            token.name as token_name,
            app_contracts.name as name,
            coalesce(token.app, app_contracts.app) as app,
            coalesce(token.friendly_name, app_contracts.friendly_name) as friendly_name,
            coalesce(token.sub_category, app_contracts.sub_category) as sub_category,
            case
                when program_id = '11111111111111111111111111111111'
                then 'EOA'
                when token.category is not null
                then token.category
                when program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
                then 'Token'
                when app_contracts.category is not null
                then app_contracts.category
                else null
            end as category,
            sybil.user_type,
            sybil.address_life_span,
            sybil.cur_total_txns,
            sybil.cur_distinct_to_address_count,
            sybil.probability,
            sybil.engagement_type,
            bal.balance_usd,
            bal.native_token_balance,
            bal.stablecoin_balance
        from chain_transactions as t
        left join app_contracts on lower(t.program_id) = lower(app_contracts.address)
        left join app_contracts as token on lower(token_address) = lower(token.address)
        left join
            balances as bal on signers[0]::string = bal.address and raw_date = bal.date
        left join collapsed_prices on raw_date = collapsed_prices.date
        left join
            {{ ref("dim_solana_sybil_address") }} as sybil
            on t.signers[0] = sybil.from_address
    )
select
    tx_hash,
    max(raw_date) as raw_date,
    max(block_timestamp) as block_timestamp,
    any_value(signers) as signers,
    max(program_id) as program_id,
    max(chain) as chain,
    max(tx_fee) as tx_fee,
    max(gas_usd) as gas_usd,
    max(succeeded) as succeeded,
    max(token_address) as token_address,
    max(token_name) as token_name,
    max(name) as name,
    max(app) as app,
    max(friendly_name) as friendly_name,
    max(sub_category) as sub_category,
    max(category) as category,
    max(user_type) as user_type,
    max(address_life_span) as address_life_span,
    max(cur_total_txns) as cur_total_txns,
    max(cur_distinct_to_address_count) as cur_distinct_to_address_count,
    max(balance_usd) as balance_usd,
    max(native_token_balance) as native_token_balance,
    max(stablecoin_balance) as stablecoin_balance,
    max(probability) AS probability,
    max(engagement_type) AS engagement_type
from tagged_transactions
group by tx_hash
