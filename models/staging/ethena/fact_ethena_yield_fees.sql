with yield_fees as (
    select distinct 
        block_timestamp, 
        transaction_hash, 
        contract_address, 
        f.this, 
        f.this:"from"::string as from_address, 
        f.this:"to"::string as to_address, 
        f.this:"value"::float as raw_amount
    from {{ref('fact_ethereum_decoded_events')}}, 
        lateral flatten(input => decoded_log) as f
    where event_name = 'Transfer' 
        and lower(f.this:"from"::string) = lower('0x71e4f98e8f20c88112489de3dded4489802a3a87') 
        and contract_address in (
                                    lower('0x6B175474E89094C44Da98b954EedeAC495271d0F'),
                                    lower('0x71E4f98e8f20C88112489de3DDEd4489802a3A87'),
                                    lower('0xdAC17F958D2ee523a2206206994597C13D831ec7')
                                )
    order by block_timestamp desc
)

select
    date(yf.block_timestamp) as date, 
    sum(case when lower(to_address) = lower('0xf2fa332bd83149c66b09b45670bce64746c6b439') then raw_amount/pow(10, decimals) else 0 end) as service_fee_allocation, 
    sum(case when lower(to_address) = lower('0x2b5ab59163a6e93b4486f6055d33ca4a115dd4d5') then raw_amount/pow(10, decimals) else 0 end) as foundation_fee_allocation, 
    sum(raw_amount/pow(10, decimals)) as fees
from yield_fees as yf
left join {{ref('dim_coingecko_token_map')}} as tm
    on lower(yf.contract_address) = lower(tm.contract_address)
group by date(yf.block_timestamp)
order by date desc