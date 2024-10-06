select 
    date(block_timestamp) as date,
    raw_amount / pow(10, m.decimals) as fees
from ethereum_flipside.core.fact_token_transfers t
left join ethereum_flipside.core.dim_contracts m on t.contract_address = m.address
where from_address = lower('0x71e4f98e8f20c88112489de3dded4489802a3a87')
and contract_address in (
    lower('0x6B175474E89094C44Da98b954EedeAC495271d0F'),
    lower('0x71E4f98e8f20C88112489de3DDEd4489802a3A87'),
    lower('0xdAC17F958D2ee523a2206206994597C13D831ec7')
)