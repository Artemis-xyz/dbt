{{
    config(
        materialized="table",
        snowflake_warehouse="SEI",
    )
}}
select wasm_contract_address , evm_contract_address
from
    (
        values
            ('ibc/CA6FBFAF399474A06263E10D0CE5AEBBE15189D6D4B2DD9ADE61007E68EB9DB0', '0x3894085Ef7Ff0f0aeDf52E2A2704928d1Ec074F1'),
            ('ibc/6C00E4AA0CC7618370F81F7378638AE6C48EFF8C9203CE1C2357012B440EBDB7', '0xB75D0B03c06A926e488e2659DF1A861F860bD3d1')
    ) as results(wasm_contract_address , evm_contract_address)