{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM",
    )
}}

with new_address as (
    {{ forward_filled_balance_for_address(
        chain='ethereum',
        address='0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe',
    )}}
)
, old_address as (  
    -- Address that seeded the above address' funds
   {{forward_filled_balance_for_address(
        chain='ethereum',
        address='0x5AbFEc25f74Cd88437631a7731906932776356f9', 
    )}}
)
, grant_provider as (
    -- EF Grants address
   {{forward_filled_balance_for_address(
        chain='ethereum',
        address='0x9eE457023bB3De16D51A003a247BaEaD7fce313D',
    )}}
)
select date, contract_address, address, balance_native, balance_raw from new_address
UNION ALL
select date, contract_address, address, balance_native, balance_raw from old_address
UNION ALL
select date, contract_address, address, balance_native, balance_raw from grant_provider
UNION ALL
-- Hardcode missing dates
SELECT *
FROM VALUES 
    (DATE '2015-08-08','eip155:1:native', '0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe', 11901464.23948, 11901464.23948),
    (DATE '2015-08-09','eip155:1:native', '0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe', 11901464.23948, 11901464.23948),
    (DATE '2015-08-10','eip155:1:native', '0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe', 11901464.23948, 11901464.23948),
    (DATE '2015-08-11','eip155:1:native', '0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe', 11901464.23948, 11901464.23948)
AS t(date, contract_address, address, balance_native, balance_raw)