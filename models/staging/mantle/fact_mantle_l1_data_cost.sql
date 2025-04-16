{{ config(materialized="incremental", unique_key="date") }}
with
    mantle_data as (
        select raw_date as date, sum(tx_fee) as fees_native, sum(gas_usd) as fees
        from {{ ref("fact_ethereum_transactions_v2") }}
        where
            lower(from_address) in (
                lower('0x4e59e778a0fb77fBb305637435C62FaeD9aED40f'),
                lower('0x5a021DC06A9630bb56099b8aEdfaDC2dEa7eB317'),
                lower('0x207E804758e28F2b3fD6E4219671B327100b82f8'),
                lower('0x6667961f5e9C98A76a48767522150889703Ed77D'),
                lower('0x2F6AFE2E3feA041b892a6e240Fd1A0E5b51e8376'),
                lower('0xE8Bf1c5750354694eD75f97B549cF570fA516725'),
                lower('0x354583673E99B48541Ec4cCBbc5e16bAfD8E9D3e'),
                lower('0x2f40D796917ffB642bD2e2bdD2C762A5e40fd749'),
                lower('0x630E93130809c4acE55249229Ee6B043b929A9Ad'),
                lower('0x2A2954F3989a83Cc43DD58B0f038D5F276f21333'),
                lower('0x9436b6F211f50F5d5a17521e4725E27518dAFA61'),
                lower('0xBB08BEEaeD39cbD6D9BdB24F2627092984ae55a1'),
                lower('0x6aBa10C9BB6a39445d990Fb7A720fA53025Bd7Dc'),
                lower('0xc828De4e817e1705CC539267D5F28BE8dbeF28DE'),
                lower('0x306d1c4482b629a684A54B121F6f57dbE617740b'),
                lower('0xBbDbE7F5aC524078ac72c1d4d39cD9b40cb4b9aD'),
                lower('0xd4fa4b67b69C3FcF3dbF15db5d80202040BC72Fc'),
                lower('0xf995d1022269c2e0ecaacD15d73e3a621FD69A3E'),
                lower('0x51A887f3C12295A69f57D0f41b276bfEBc91AafC'),
                lower('0x18214cCF63060d579524A69ee1B97404594fc831'),
                lower('0x9844f67c071caE21B0324D049Febe67158E23f98'),
                lower('0x1888e4aC2Ab37A73B33448B87bABdD1ce1dcBAbe'),
                lower('0x717c3DC6Df69c316d6Ac593077BC84Cc86f214A4'),
                lower('0x8BEF0466b7C2CbFD753eF340e062dF06E93ADA7f'),
                lower('0xc1dEd495E1dDf089B2b41d6397C0aBa04BDA1A21'),
                lower('0x6cc5A6F5a9E4757790e4068Aa9757226Cb854B64'),
                lower('0x550b3CB2D5fB5E4F0A08322CaC7b04291558CDa8'),
                lower('0x8A3D6c77E5BAcE8cb0822B28E4Fc56FC06fB5645'),
                lower('0xB61298691FE0df10634A67dd83b2253E74cbF7fb'),
                lower('0xcEb157a9bB9c80a845d5924e8CEAA591Caf705a5'),
                lower('0x0B6F2C77C3740A5e6f88A4eCdd02C10BE8a2e323'),
                lower('0x4D2A6Ac5928723DEC0e9A768c8413790e9F6428e'),
                lower('0x2669F422FC59c0e75092F46ea9783bb54D4C04cd'),
                lower('0x4c5E208BA71d457bD18cFc7f1847DA7209C79994'),
                lower('0x47336ae44F573a7C3C41a9ae04A9D48E5dFD8f8E'),
                lower('0x84A628347537d4900a0b720Ee294445F90c3887a'),
                lower('0x47336ae44F573a7C3C41a9ae04A9D48E5dFD8f8E'),
                lower('0x84A628347537d4900a0b720Ee294445F90c3887a'),
                lower('0x769F18454F6Af97Df35b4839F89EB9F829eD4bA9'),
                lower('0xE3ff520cb5C38c36D92b048abe39A1c9F0090238'),
                lower('0x49426759d1F8C757f4df9E7CBb150EB00B31278A'),
                lower('0x95aC4B3c526635ccE9DD12b1340073C748eB6792'),
                lower('0x1CF151da13A460BBCd70bFE3f6c7e9241aaA6e94'),
                lower('0xCF2F72CD9d4A055Ba72eBC523Bfd58bCDbe84C7d'),
                lower('0xB2B9807A98731f77cFd190666eB941B1dB132ecB'),
                lower('0x2368500Cf2A56bB497246f7606b3f72784A0783D'),
                lower('0x47336ae44F573a7C3C41a9ae04A9D48E5dFD8f8E'),
                lower('0x84A628347537d4900a0b720Ee294445F90c3887a'),
                lower('0x769F18454F6Af97Df35b4839F89EB9F829eD4bA9'),
                lower('0xE3ff520cb5C38c36D92b048abe39A1c9F0090238'),
                lower('0x49426759d1F8C757f4df9E7CBb150EB00B31278A'),
                lower('0x95aC4B3c526635ccE9DD12b1340073C748eB6792'),
                lower('0x1CF151da13A460BBCd70bFE3f6c7e9241aaA6e94'),
                lower('0xCF2F72CD9d4A055Ba72eBC523Bfd58bCDbe84C7d'),
                lower('0xB2B9807A98731f77cFd190666eB941B1dB132ecB'),
                lower('0x2368500Cf2A56bB497246f7606b3f72784A0783D'),
                lower('0x2F44BD2a54aC3fB20cd7783cF94334069641daC9'),
                lower('0x4e59e778a0fb77fBb305637435C62FaeD9aED40f')
            )
        group by raw_date
        order by raw_date desc
    )
select
    mantle_data.date,
    coalesce(mantle_data.fees_native, 0) as l1_data_cost_native,
    coalesce(mantle_data.fees, 0) as l1_data_cost,
    'mantle' as chain
from mantle_data
where mantle_data.date < to_date(sysdate())
{% if is_incremental() %} 
    and mantle_data.date >= (
        select dateadd('day', -3, max(date))
        from {{ this }}
    )
{% endif %}


