

with
max_extraction as (
    select max(extraction_date) as max_date
    from {{ source("PROD_LANDING", "raw_icp_daily_stats") }}
)
,latest_data as (
    select parse_json(source_json) as data
    from {{ source("PROD_LANDING", "raw_icp_daily_stats") }}
    where extraction_date = (select max_date from max_extraction)
)
,icp_expanded_data as (
    select
        value:day::date as date
        ,value:internet_identity_user_count::int as total_internet_identity_user_count
        ,value:unique_accounts_per_day::int as dau
        ,value:average_transactions_per_second::float as avg_tps
        ,value:blocks_per_second_average::float as avg_blocks_per_second
        ,value:icp_burned_total::int / 10e7 as icp_burned_total
        ,LAG(value:icp_burned_total, 1, null) OVER (ORDER BY value:day::date) / 10e7 as prev_icp_burned_total
        ,value:icp_burned_fees::int / 10e7 as icp_burned_fees
        ,value:governance_neurons_total::int as neurons_total
        ,value:governance_neuron_fund_total_staked_e8s::int / 10e7 as nns_total_staked
        ,value:governance_total_locked_e8s::int / 10e7 as nns_tvl
        ,value:proposals_count::int as total_proposals_count
        ,value:registered_canisters_count::int as total_registered_canister_count
        ,value:total_transactions::int as total_transactions
        -- DQ issues where estimated returns are sometimes >> 1 Trillion
        , case 
            when 
                abs(
                    ln(abs(coalesce(value:estimated_rewards_percentage:"1_year"::float, 1))) / ln(10)
                    - ln(abs(coalesce(lag(value:estimated_rewards_percentage:"1_year"::float) OVER (ORDER BY value:day::date), 1))) / ln(10)
                )
                < 2
                and 
                abs(
                    ln(abs(coalesce(value:estimated_rewards_percentage:"1_year"::float, 1))) / ln(10)
                    - ln(abs(coalesce(lead(value:estimated_rewards_percentage:"1_year"::float) OVER (ORDER BY value:day::date), 1))) / ln(10)
                )
                < 2
            then value:estimated_rewards_percentage:"1_year"::float
            else null
        end as one_year_staking_apy
        ,value:ckbtc_total_supply::int / 10e7 as ckbtc_total_supply
        ,value:cycle_burn_rate_average::int as cycle_burn_rate_average
        ,value:canister_memory_usage_bytes::int as canister_memory_usage_bytes
    from latest_data, lateral flatten(input => data) as f
)
select 
    date
    , total_transactions
    , dau
    , total_transactions - LAG(total_transactions, 1, null) OVER (ORDER BY date) as txns
    , neurons_total
    , avg_tps
    , avg_blocks_per_second
    , case 
        when icp_burned_total - prev_icp_burned_total < 0  or icp_burned_total - prev_icp_burned_total > 100000
        then 0
        else icp_burned_total - prev_icp_burned_total
      end as icp_burned
    , icp_burned_total as total_icp_burned
    , icp_burned_fees as total_native_fees -- total transaction fees
    , nns_tvl as nns_tvl_native -- same as total icp staked in NNS
    , total_proposals_count - LAG(total_proposals_count, 1, null) OVER (ORDER BY date) as nns_proposal_count
    , total_registered_canister_count -- total cannister count 
    , canister_memory_usage_bytes / 10e9 as canister_memory_usage_gb -- cannister state
    , one_year_staking_apy
    , ckbtc_total_supply
    , cycle_burn_rate_average
    , total_internet_identity_user_count
    , 'internet_computer' as chain
from icp_expanded_data
where dau is not null 
