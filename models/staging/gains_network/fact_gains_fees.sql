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
    , value:project_fund::number as treasury_cash_flow -- treasury_cash_flow
    , value:gns_stakers::number as gns_stakers -- this was fee_sharing_token_cash_flow. Then it turned into buybacks. 90% went to buyback. 10% went to treasury_cash_flow
    , value:dev_fund::number as foundation_cash_flow -- foundation_cash_flow
    , value:dai_stakers::number as dai_stakers  -- fee_sharing_token_cash_flow 
    , value:lp_stakers::number as service_cash_flow  -- service_cash_flow
    , value:referral::number as referral_fees -- referral_fees
    , value:nft_bots::number as nft_bot_fees --nft_bot_fees
from
    {{ source("PROD_LANDING", "raw_gains_fees") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
