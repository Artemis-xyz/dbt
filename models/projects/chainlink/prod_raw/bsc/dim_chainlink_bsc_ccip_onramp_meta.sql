{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="dim_bsc_ccip_onramp_meta",
    )
}}



SELECT 
    chain
    , onramp AS onramp
    , chain_selector::number AS chain_selector
    , version
FROM (VALUES 
 ('ethereum', '0x1f17D464652f5Bd74a03446FeA20590CCfB3332D', '5009297550715157269', 'v1.0.0')
, ('optimism', '0x0000000000000000000000000000000000000000', '3734403246176062136', 'v1.0.0')
, ('polygon', '0xCAd54BE1A4Bc5e467cd5B53896eb692D9f6956cD', '4051577828743386545', 'v1.0.0')
, ('arbitrum', '0x0000000000000000000000000000000000000000', '4949039107694359620', 'v1.0.0')
, ('avalanche', '0xf7c9B607cF09B4048f09C84236cE7f11DF6D6364', '6433500567565415381', 'v1.0.0')
, ('base', '0xFdc26aA261655580f7ac413927983F664291Fd22', '15971525489660198786', 'v1.0.0')
, ('ethereum', '0x0Bf40b034872D0b364f3DCec04C7434a4Da1C8d9', '5009297550715157269', 'v1.2.0')
, ('optimism', '0x4FEB11A454C9E8038A8d0aDF599Fe7612ce114bA', '3734403246176062136', 'v1.2.0')
, ('polygon', '0x6bD4754D86fc87FE5b463D368f26a3587a08347c', '4051577828743386545', 'v1.2.0')
, ('arbitrum', '0x2788b46BAcFF49BD89562e6bA5c5FBbbE5Fa92F7', '4949039107694359620', 'v1.2.0')
, ('avalanche', '0x6aa72a998859eF93356c6521B72155D355D0Cfd2', '6433500567565415381', 'v1.2.0')
, ('base', '0x70bC7f7a6D936b289bBF5c0E19ECE35B437E2e36', '15971525489660198786', 'v1.2.0')
, ('wemix', '0x1467fF8f249f5bc604119Af26a47035886f856BE', '5142893604156789321', 'v1.2.0')
)
AS a (chain, onramp, chain_selector, version)