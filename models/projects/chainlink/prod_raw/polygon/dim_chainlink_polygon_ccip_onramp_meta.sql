{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_polygon_ccip_onramp_meta",
    )
}}



SELECT 
    chain
    , onramp AS onramp
    , chain_selector::number AS chain_selector
    , version
FROM (VALUES 
('ethereum', '0xAE0e486Fa6577188d586A8e4c12360FB82E2a386', '5009297550715157269', 'v1.0.0')
, ('optimism', '0xD8E79DeF51a98b71c98b4C19D4A314341670AC36', '3734403246176062136', 'v1.0.0')
, ('arbitrum', '0x0000000000000000000000000000000000000000', '4949039107694359620', 'v1.0.0')
, ('avalanche', '0x47D945f7bbb814B65775a89c71F5D2229BE96CE9', '6433500567565415381', 'v1.0.0')
, ('base', '0x0000000000000000000000000000000000000000', '15971525489660198786', 'v1.0.0')
, ('bsc', '0xFFAacDD8FB3aF6aDa58AbABAEc549587C81351BF', '11344663589394136015', 'v1.0.0')
, ('ethereum', '0xFd77c53AA4eF0E3C01f5Ac012BF7Cc7A3ECf5168', '5009297550715157269', 'v1.2.0')
, ('optimism', '0x3111cfbF5e84B5D9BD952dd8e957f4Ca75f728Cf', '3734403246176062136', 'v1.2.0')
, ('arbitrum', '0xD16D025330Edb91259EEA8ed499daCd39087c295', '4949039107694359620', 'v1.2.0')
, ('avalanche', '0x5FA30697e90eB30954895c45b028F7C0dDD39b12', '6433500567565415381', 'v1.2.0')
, ('base', '0x20B028A2e0F6CCe3A11f3CE5F2B8986F932e89b4', '15971525489660198786', 'v1.2.0')
, ('bsc', '0xF5b5A2fC11BF46B1669C3B19d98B19C79109Dca9', '11344663589394136015', 'v1.2.0')
, ('wemix', '0x5060eF647a1F66BE6eE27FAe3046faf8D53CeB2d', '5142893604156789321', 'v1.2.0')
)
AS a (chain, onramp, chain_selector, version)