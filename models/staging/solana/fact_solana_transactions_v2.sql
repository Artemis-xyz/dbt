{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BAM_TRANSACTION_XLG",
    )
}}

with
    solana_transfers as (
        select
            tx_id,
            min_by(value:"programId"::string, index) as program_id,
            array_agg(value:"parsed":"info":"destination"::string) as destinations
        from solana_flipside.core.fact_transactions, lateral flatten(instructions)
        where
            value:"programId"::string in ('11111111111111111111111111111111', 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA') and ARRAY_SIZE(instructions) <= 3
        -- Chunking required here for backfills
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
    instruction_data as (
        select
            tx_id,
            min_by(value:"programId"::string, index) as program_id,
        from solana_flipside.core.fact_transactions, lateral flatten(instructions)
        where
            value:"programId"::string not in (
                'ComputeBudget111111111111111111111111111111',
                'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
                'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr',
                'Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo',
                '11111111111111111111111111111111',
                'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
            )
            -- Chunking required here for backfills
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
    chain_transactions as (
        select 
            t.tx_id,
            block_timestamp,
            signers,
            fee,
            succeeded,
            coalesce(i.program_id, transfers.program_id) as program_id,
            t.post_token_balances,
            destinations
        from solana_flipside.core.fact_transactions as t
        left join instruction_data as i on t.tx_id = i.tx_id
        left join solana_transfers as transfers on t.tx_id = transfers.tx_id
        {% if is_incremental() %}
            where block_timestamp
            >= (select dateadd('day', -5, max(block_timestamp)) from {{ this }})
        {% else %}
        -- Making code not compile on purpose. Full refresh of entire history
        -- takes too long, doing last month will wipe out backfill
        -- TODO: Figure out a workaround.
        and
            block_timestamp
            >= (select dateadd('month', -1, max(block_timestamp)) from {{ this }})
        {% endif %}
    ),
    app_contracts as (
        select distinct
            address,
            contract.name,
            contract.chain,
            contract.artemis_category_id as category,
            contract.artemis_sub_category_id as sub_category,
            contract.artemis_application_id as app,
            contract.friendly_name
        from {{ ref("dim_all_addresses_labeled_gold") }} as contract
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
    tagged_transactions as (
        select
            t.tx_id as tx_hash,
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
                when program_id = '11111111111111111111111111111111' and array_size(destinations) = 1
                then 'EOA'
                when token.category is not null
                then token.category
                when program_id = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' and array_size(destinations) = 1
                then 'Token'
                when app_contracts.category is not null
                then app_contracts.category
                else null
            end as category,
            -- deprecating sybil and token balances by address
            null as user_type,
            null as address_life_span,
            null as cur_total_txns,
            null as cur_distinct_to_address_count,
            null as probability,
            null as engagement_type,
            null as balance_usd,
            null as native_token_balance,
            null as stablecoin_balance
        from chain_transactions as t
        left join app_contracts on lower(t.program_id) = lower(app_contracts.address)
        left join app_contracts as token on lower(token_address) = lower(token.address)
        left join collapsed_prices on raw_date = collapsed_prices.date
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
