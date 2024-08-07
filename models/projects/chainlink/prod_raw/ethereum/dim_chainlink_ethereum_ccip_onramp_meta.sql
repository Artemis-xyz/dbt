{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_ethereum_ccip_onramp_meta",
    )
}}



SELECT 
    chain
    , onramp AS onramp
    , chain_selector::number AS chain_selector
    , version
FROM (VALUES 
('optimism', '0xCC19bC4D43d17eB6859F0d22BA300967C97780b0', '3734403246176062136', 'v1.0.0')
, ('polygon', '0x0f27c8532457b66D6037141DEB0ed479Dad04B3c', '4051577828743386545', 'v1.0.0')
, ('arbitrum', '0x333f976915195ba9044fD0cd603cEcE936f6264e', '4949039107694359620', 'v1.0.0')
, ('avalanche', '0xd0B5Fc9790a6085b048b8Aa1ED26ca2b3b282CF2', '6433500567565415381', 'v1.0.0')
, ('base', '0xe2Eb229e88F56691e96bb98256707Bc62160FE73', '15971525489660198786', 'v1.0.0')
, ('bsc', '0xdF1d7FD22aC3aB5171E275796f123224039f3b24', '11344663589394136015', 'v1.0.0')
, ('optimism', '0x86B47d8411006874eEf8E4584BdFD7be8e5549d1', '3734403246176062136', 'v1.2.0')
, ('polygon', '0x35F0ca9Be776E4B38659944c257bDd0ba75F1B8B', '4051577828743386545', 'v1.2.0')
, ('arbitrum', '0x925228D7B82d883Dde340A55Fe8e6dA56244A22C', '4949039107694359620', 'v1.2.0')
, ('avalanche', '0x3df8dAe2d123081c4D5E946E655F7c109B9Dd630', '6433500567565415381', 'v1.2.0')
, ('base', '0xe2c2AB221AA0b957805f229d2AA57fBE2f4dADf7', '15971525489660198786', 'v1.2.0')
, ('bsc', '0x91D25A56Db77aD5147437d8B83Eb563D46eBFa69', '11344663589394136015', 'v1.2.0')
, ('wemix', '0xCbE7e5DA76dC99Ac317adF6d99137005FDA4E2C4', '5142893604156789321', 'v1.2.0')
)
AS a (chain, onramp, chain_selector, version)