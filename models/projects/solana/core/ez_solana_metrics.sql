{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="SOLANA_XLG",
        database="solana",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    contract_data as ({{ get_contract_metrics("solana") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("solana") }}),
    defillama_data as ({{ get_defillama_metrics("solana") }}),
    github_data as ({{ get_github_metrics("solana") }}),
    price as ({{ get_coingecko_metrics("solana") }}),
    staking_data as ({{ get_staking_metrics("solana") }}),
    issuance_data as (
        select date, chain, issuance from {{ ref("fact_solana_issuance_silver") }}
    ),
    {% if not is_incremental() %}
        unrefreshed_data as (
            select
                date_trunc('day', block_timestamp) as date,
                sum(case when index = 0 then fee / pow(10, 9) else 0 end) gas,
                sum(
                    case
                        when index = 0 then (array_size(signers) * (5000 / 1e9)) else 0
                    end
                ) as base_fee_native,
                count_if(index = 0 and succeeded = 'TRUE') as txns,
                count(
                    distinct(case when succeeded = 'TRUE' then value else null end)
                ) dau,
                null as returning_users,
                null as new_users
            from
                solana_flipside.core.fact_transactions,
                lateral flatten(input => signers)
            where
                date_trunc('day', block_timestamp)
                < (select min(raw_date) from {{ ref("ez_solana_transactions") }})
            group by date
        ),
        unrefreshed_data_with_price as (
            select
                unrefreshed_data.date,
                gas,
                gas * price as gas_usd,
                base_fee_native,
                txns,
                dau,
                returning_users,
                new_users
            from unrefreshed_data
            left join price on unrefreshed_data.date = price.date
        ),
    {% endif %}
    min_date as (
        select min(raw_date) as start_date, value as signer
        from {{ ref("ez_solana_transactions") }}, lateral flatten(input => signers)
        where succeeded = 'TRUE'
        group by signer
    ),
    new_users as (
        select count(distinct signer) as new_users, start_date
        from min_date
        group by start_date
    ),
    voting_fees as (
        select
            date_trunc('day', block_timestamp) as date,
            sum(num_votes * 5000) / pow(10, 9) as vote_tx_fee_native
        from solana_flipside.gov.fact_votes_agg_block
        {% if is_incremental() %}
            where
                date_trunc('day', block_timestamp)
                > (select dateadd('day', -5, max(date)) from {{ this }})
        {% endif %}
        group by date
    ),
    agg_data as (
        select
            raw_date,
            max(chain) as chain,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then gas_usd else 0 end) gas_usd,
            sum(
                case when index = 0 then (array_size(signers) * (5000 / 1e9)) else 0 end
            ) as base_fee_native,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) dau
        from {{ ref("ez_solana_transactions") }}, lateral flatten(input => signers)
        {% if is_incremental() %}
            where raw_date > (select dateadd('day', -5, max(date)) from {{ this }})
        {% endif %}
        group by raw_date
    ),
    fundamental_usage as (
        select
            agg_data.raw_date as date,
            gas,
            gas_usd,
            base_fee_native,
            txns,
            dau,
            (dau - new_users) as returning_users,
            new_users
        from agg_data
        left join new_users on date = new_users.start_date
        {% if not is_incremental() %}
            union
            select
                date,
                gas,
                gas_usd,
                base_fee_native,
                txns,
                dau,
                returning_users,
                new_users
            from unrefreshed_data_with_price
        {% endif %}
    )
select
    fundamental_usage.date,
    'solana' as chain,
    base_fee_native,
    base_fee_native * price as base_fee,
    case
        when (gas - base_fee_native) < 0.00001 then 0 else (gas - base_fee_native)
    end as priority_fee_native,
    case
        when (gas_usd - base_fee) < 0.001 then 0 else (gas_usd - base_fee)
    end as priority_fee,
    vote_tx_fee_native,
    vote_tx_fee_native * price as vote_tx_fee_usd,
    gas + vote_tx_fee_native as fees_native,
    vote_tx_fee_usd + gas_usd as fees,
    gas_usd / txns as avg_txn_fee,
    fees_native * .5 as revenue_native,
    fees * .5 as revenue,
    price,
    market_cap,
    fdmc,
    tvl,
    dex_volumes,
    -- NOTE: txns only contains non-votes, votes can only be referenced explicitly in
    -- fact_votes_agg_block
    txns,
    dau,
    returning_users,
    new_users,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem,
    weekly_contracts_deployed,
    weekly_contract_deployers,
    stablecoin_total_supply,
    stablecoin_txns,
    stablecoin_dau,
    stablecoin_transfer_volume,
    total_staked_native,
    total_staked_usd,
    issuance
from fundamental_usage
left join defillama_data on fundamental_usage.date = defillama_data.date
left join stablecoin_data on fundamental_usage.date = stablecoin_data.date
left join voting_fees on fundamental_usage.date = voting_fees.date
left join price on fundamental_usage.date = price.date
left join github_data on fundamental_usage.date = github_data.date
left join contract_data on fundamental_usage.date = contract_data.date
left join staking_data on fundamental_usage.date = staking_data.date
left join issuance_data on fundamental_usage.date = issuance_data.date
where fundamental_usage.date < to_date(sysdate())
