{{
    config(
        materialized="table",
        snowflake_warehouse="GAINS_NETWORK"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_gains_fees") }}
    )
select
    left(value:day, 10)::date as date
    , value:all_fees::number as fees
    , value:revenue::number as revenue
    , value:project_fund::number as treasury_fee_allocation -- treasury_fee_allocation
    , value:gns_stakers::number as gns_stakers -- this was staking_fee_allocation. Then it turned into buybacks. 90% went to buyback. 10% went to treasury_fee_allocation
    , value:dev_fund::number as foundation_fee_allocation -- foundation_fee_allocation
    , value:dai_stakers::number as dai_stakers  -- staking_fee_allocation 
    , value:lp_stakers::number as service_fee_allocation  -- service_fee_allocation
    , value:referral::number as referral_fees -- referral_fees
    , value:nft_bots::number as nft_bot_fees --nft_bot_fees
from
    {{ source("PROD_LANDING", "raw_gains_fees") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
