{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_arbitrum_ccip_onramp_meta",
    )
}}



SELECT 
    chain
    , onramp AS onramp
    , chain_selector::number AS chain_selector
    , version
FROM (VALUES 
 ('ethereum', '0x98dd9E9b8AE458225119Ab5B8c947A9d1cd0B648', '5009297550715157269', 'v1.0.0')
 ,('ethereum', '0x6264f5c5bc1c0201159a5bcd6486d9c6c2f75439', '5009297550715157269', 'v1.0.0') -- found through traces (old)
, ('optimism', '0x0000000000000000000000000000000000000000', '3734403246176062136', 'v1.0.0')
, ('avalanche', '0x0000000000000000000000000000000000000000', '6433500567565415381', 'v1.0.0')
, ('polygon', '0x0000000000000000000000000000000000000000', '4051577828743386545', 'v1.0.0')
, ('polygon', '0x9409b222c96ae8377db6a4b6645350f7dc94e9ef', '4051577828743386545', 'v1.0.0') -- found through traces (old)
, ('base', '0x590791aA846eC4D2Aa2B8697Edeb6158F6054839', '15971525489660198786', 'v1.0.0')
, ('bsc', '0x0000000000000000000000000000000000000000', '11344663589394136015', 'v1.0.0')
, ('ethereum', '0xCe11020D56e5FDbfE46D9FC3021641FfbBB5AdEE', '5009297550715157269', 'v1.2.0')
, ('optimism', '0xC09b72E8128620C40D89649019d995Cc79f030C3', '3734403246176062136', 'v1.2.0')
, ('avalanche', '0x05B723f3db92430FbE4395fD03E40Cc7e9D17988', '6433500567565415381', 'v1.2.0')
, ('polygon', '0x122F05F49e90508F089eE8D0d868d1a4f3E5a809', '4051577828743386545', 'v1.2.0')
, ('base', '0x77b60F85b25fD501E3ddED6C1fe7bF565C08A22A', '15971525489660198786', 'v1.2.0')
, ('bsc', '0x79f3ABeCe5A3AFFf32D47F4CFe45e7b65c9a2D91', '11344663589394136015', 'v1.2.0')
, ('wemix', '0x66a0046ac9FA104eB38B04cfF391CcD0122E6FbC', '5142893604156789321', 'v1.2.0')
)
AS a (chain, onramp, chain_selector, version)