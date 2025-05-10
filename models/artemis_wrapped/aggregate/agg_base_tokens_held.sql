{{config(materialized='table')}}
--WOLF OF WALLSTREET
--RUGGED RAT
with
    tokens as (
        select contract_address, symbol
        from (
            values
                ('0x4ed4e862860bed51a9570b96d89af5e1b0efefed', 'DEGEN')
                , ('0x52b492a33e447cdb854c7fc19f1e57e8bfa1777d', 'PEPE')
                , ('0xb1a03eda10342529bbf8eb700a06c60441fef25d', 'MIGGLES')
                , ('0xac1bd2486aaf3b5c0fc3fd868558b082a531b2b4', 'TOSHI')
                , ('0x9a26f5433671751c3276a065f57e5a02d2817973', 'KEYCAT')
                , ('0x2f20cf3466f80a5f7f532fca553c8cbc9727fef6', 'AKUMA')
                , ('0xfad8cb754230dbfd249db0e8eccb5142dd675a0d', 'AEROBUD')
                , ('0x23a96680ccde03bd4bdd9a3e9a0cb56a5d27f7c9', 'HENLO')
                , ('0x6921b130d297cc43754afba22e5eac0fbf8db75b', 'DOGINME')
                , ('0x768be13e1680b5ebe0024c42c896e3db59ec0149', 'SKI')
                , ('0x532f27101965dd16442e59d40670faf5ebb142e4', 'BRETT')
            ) as t(contract_address, symbol)
    )
select lower(address) as address, symbol, min(block_timestamp) as first_seen, coalesce(max(block_timestamp), sysdate()) as last_interaction_timestamp
from {{ ref('fact_base_address_balances') }} t
inner join tokens on lower(t.contract_address) = lower(tokens.contract_address)
where block_timestamp > '2023-12-31' 
group by 1, 2