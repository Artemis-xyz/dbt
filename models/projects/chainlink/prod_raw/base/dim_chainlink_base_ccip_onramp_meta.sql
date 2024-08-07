{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_base_ccip_onramp_meta",
    )
}}



SELECT 
    chain
    , onramp AS onramp
    , chain_selector::number AS chain_selector
    , version
FROM (VALUES 
 ('ethereum', '0xD44371bFDe87f2db3eA6Df242091351A06c2e181', '5009297550715157269', 'v1.0.0')
, ('optimism', '0xe96563b8a6b4eA245e7fCEFaba813104FC889C6c', '3734403246176062136', 'v1.0.0')
, ('polygon', '0x0000000000000000000000000000000000000000', '4051577828743386545', 'v1.0.0')
, ('bsc', '0x064f0960Ab66F44A5e6c7D2335b19De4Bb75AA0D', '11344663589394136015', 'v1.0.0')
, ('arbitrum', '0x223953DB4E0A4C33bac1B17B0df1c22919984c60', '4949039107694359620', 'v1.0.0')
, ('avalanche', '0x0000000000000000000000000000000000000000', '6433500567565415381', 'v1.0.0')
, ('ethereum', '0xDEA286dc0E01Cb4755650A6CF8d1076b454eA1cb', '5009297550715157269', 'v1.2.0')
, ('optimism', '0xd952FEAcDd5919Cc5E9454b53bF45d4E73dD6457', '3734403246176062136', 'v1.2.0')
, ('polygon', '0x3DB8Bea142e41cA3633890d0e5640F99a895D6A5', '4051577828743386545', 'v1.2.0')
, ('bsc', '0xdd4Fb402d41Beb0eEeF6CfB1bf445f50bDC8c981', '11344663589394136015', 'v1.2.0')
, ('arbitrum', '0x1E5Ca70d1e7A1B26061125738a880BBeA42FeB21', '4949039107694359620', 'v1.2.0')
, ('avalanche_c', '0xBE5a9E336D9614024B4Fa10D8112671fc9A42d96', '6433500567565415381', 'v1.2.0')
)
AS a (chain, onramp, chain_selector, version)