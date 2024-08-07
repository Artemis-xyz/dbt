{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_avalanche_ccip_onramp_meta",
    )
}}



SELECT 
    chain
    , onramp AS onramp
    , chain_selector::number AS chain_selector
    , version
FROM (VALUES 
 ('ethereum', '0x3D3817270db2b89e9F68bA27297fb4672082f942', '5009297550715157269', 'v1.0.0')
, ('optimism', '0x0000000000000000000000000000000000000000', '3734403246176062136', 'v1.0.0')
, ('polygon', '0x2d306510FE83Cdb33Ff1658c71C181e9567F0009', '4051577828743386545', 'v1.0.0')
, ('base', '0x0000000000000000000000000000000000000000', '15971525489660198786', 'v1.0.0')
, ('bsc', '0x5c7AD3715257D20F2ae8596af55203373128BeE1', '11344663589394136015', 'v1.0.0')
, ('arbitrum', '0x0000000000000000000000000000000000000000', '4949039107694359620', 'v1.0.0')
, ('ethereum', '0xD0701FcC7818c31935331B02Eb21e91eC71a1704', '5009297550715157269', 'v1.2.0')
, ('optimism', '0x8629008887E073260c5434D6CaCFc83C3001d211', '3734403246176062136', 'v1.2.0')
, ('polygon', '0x97500490d9126f34cf9aA0126d64623E170319Ef', '4051577828743386545', 'v1.2.0')
, ('base', '0x268fb4311D2c6CB2bbA01CCA9AC073Fb3bfd1C7c', '15971525489660198786', 'v1.2.0')
, ('bsc', '0x8eaae6462816CB4957184c48B86afA7642D8Bf2B', '11344663589394136015', 'v1.2.0')
, ('arbitrum', '0x98f51B041e493fc4d72B8BD33218480bA0c66DDF', '4949039107694359620', 'v1.2.0')
, ('wemix', '0x9b1ed9De069Be4d50957464b359f98eD0Bf34dd5', '5142893604156789321', 'v1.2.0')
)
AS a (chain, onramp, chain_selector, version)