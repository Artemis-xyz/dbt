{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="BAM_TRANSACTION_XLG",
    )
}}

with
    price as ({{ get_coingecko_price_with_latest("solana") }}),
    {% if not is_incremental() %}
        unrefreshed_data as (
            select
                date_trunc('day', block_timestamp) as date,
                'solana' as chain,
                sum(case when index = 0 then fee / pow(10, 9) else 0 end) gas,
                count_if(index = 0 and succeeded = 'TRUE') as txns,
                count(
                    distinct(case when succeeded = 'TRUE' then value else null end)
                ) daa,
                null as returning_users,
                null as new_users,
                null as dau_over_100,
                null as low_sleep_users,
                null as high_sleep_users
            from
                solana_flipside.core.fact_transactions,
                lateral flatten(input => signers)
            where
                date_trunc('day', block_timestamp)
                < (select min(raw_date) from {{ ref("fact_solana_transactions_gold") }})
            group by date
        ),
        unrefreshed_data_with_price as (
            select
                unrefreshed_data.date,
                chain,
                gas,
                gas * price as gas_usd,
                txns,
                daa,
                returning_users,
                new_users,
                dau_over_100,
                low_sleep_users,
                high_sleep_users
            from unrefreshed_data
            left join price on unrefreshed_data.date = price.date
        ),
    {% endif %}
    min_date as (
        select min(raw_date) as start_date, value as signer
        from
            {{ ref("fact_solana_transactions_gold") }},
            lateral flatten(input => signers)
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
            sum(num_votes * 5000) / pow(10, 9) as vote_tx_fee
        from solana_flipside.gov.fact_votes_agg_block
        {% if is_incremental() %}
            where
                date_trunc('day', block_timestamp)
                > (select dateadd('day', -5, max(date)) from {{ this }})
        {% endif %}
        group by date
    ),
    voting_fees_usd as (
        select voting_fees.date, vote_tx_fee, vote_tx_fee * price as vote_tx_fee_usd
        from voting_fees
        left join price on voting_fees.date = price.date
    ),
    agg_data as (
        select
            raw_date,
            max(chain) as chain,
            sum(case when index = 0 then tx_fee else 0 end) gas,
            sum(case when index = 0 then gas_usd else 0 end) gas_usd,
            count_if(index = 0 and succeeded = 'TRUE') as txns,
            count(distinct(case when succeeded = 'TRUE' then value else null end)) daa
        from
            {{ ref("fact_solana_transactions_gold") }},
            lateral flatten(input => signers)
        {% if is_incremental() %}
            where raw_date > (select dateadd('day', -5, max(date)) from {{ this }})
        {% endif %}
        group by raw_date
    ),
    fundamental_usage as (
        select
            agg_data.raw_date as date,
            chain,
            gas,
            gas_usd,
            txns,
            daa,
            (daa - new_users) as returning_users,
            new_users,
            null as dau_over_100,
            null as low_sleep_users,
            null as high_sleep_users
        from agg_data
        left join new_users on date = new_users.start_date
        {% if not is_incremental() %}
            union
            select
                date,
                chain,
                gas,
                gas_usd,
                txns,
                daa,
                returning_users,
                new_users,
                dau_over_100,
                low_sleep_users,
                high_sleep_users
            from unrefreshed_data_with_price
        {% endif %}
    )
select
    fundamental_usage.date,
    chain,
    gas,
    gas_usd,
    vote_tx_fee_usd + gas_usd as fees,
    fees * .5 as revenue,
    txns,
    daa,
    returning_users,
    new_users,
    dau_over_100,
    low_sleep_users,
    high_sleep_users
from fundamental_usage
left join voting_fees_usd on fundamental_usage.date = voting_fees_usd.date
