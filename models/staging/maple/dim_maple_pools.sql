{{ 
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
    )
}}


-- Creates a table consisting of each Maple Pool with its ID, name, activation block, Loan Managers, etc.
-- For Pools that were migrated from V1->V2, also includes the V1 contract address so we can merge all historical data for those Pools
-- Each Pool is denominated in a single asset, and we store that asset's on-chain decimal precision here (e.g. WETH is 18, USDC is 6)
SELECT 
* 
FROM 
    ( VALUES 
        ('0xc1dd3f011290f212227170f0d02f511ebf57e433', 'Secured Lending', '0x91582bdfef0bf36fc326a4ab9b59aacd61c105ff', '0xdc9b93a8a336fe5dc9db97616ea2118000d70fc0', null, 'USDC', 6, 17878025),
        ('0xc39a5a616f0ad1ff45077fa2de3f79ab8eb8b8b9', 'High Yield Secured Lending', '0x78a13c2f24df55feef7f8c895396cf1dd21cf56f', '0xb50d675f3c6d18ce5ccac691354f92afebd1675e', null, 'USDC', 6, 19363453),
        ('0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b', 'Syrup USDC', '0x4a1c3f0d9ad0b3f9da085bebfc22dea54263371b', '0x6aceb4caba81fa6a8065059f3a944fb066a10fac', null, 'USDC', 6, 19928507),
        ('0x356b8d89c1e1239cbbb9de4815c39a1474d5ba7d', 'Syrup USDT', '0xc17aa0cb662bc4787bb16bd3bc13d0d88eb7abdd', '0x616022e54324ef9c13b99c229dac8ea69af4faff', null, 'USDC', 6, 20478857),
        ('0x6174a27160f4d7885db4ffed1c0b5fbd66c87f3a', 'High Yield Corporate USDC', '0xeca9d2c5f81dd50dce7493104467dc33362a436f', '0x2638802a78d6a97d0041cc7b52fb9a80994424cd', null, 'USDC', 6, 18793090),
        ('0xccbc525ed9d85ad8325b7b6c4c6a79f5566dea3b', 'High Yield Corporate WETH', '0xc82095c002e726e4b3c8c26ee769b44c772ef9f7', '0xe3aac29001c769fafcef0df072ca396e310ed13b', null, 'WETH', 18, 19335521),
        ('0xfe119e9c24ab79f1bdd5dd884b86ceea2ee75d92', 'Maple Cash USDC1', '0xf4d4a5270aa834a2a77011526447fdf1e227018f', '0xfab269cb4ab4d33a61e1648114f6147742f5eecc', null, 'USDC', 6, 17088258),
        ('0xf05681a33a9adf14076990789a89ab3da3f6b536', 'Maple Cash USDT1', '0x1b61765e954113e6508c4f9db07675989f7f5874', '0x93b0f6f03cc6996120c19abff3e585fdb8d88648', null, 'USDT', 6, 17634073),
        ('0xe9d33286f0e37f517b1204aa6da085564414996d', 'AQRU Receivables Financing', '0xd05998a1940294e3e49f99dbb13fe20a3483f5ae', '0x483082e93635ef280bc5e9f65575a7ff288aba33', null, 'USDC', 6, 16486529),
        ('0xf025edfa685c9ea873ea4b22da85e7e1fba24381', 'Osprey Total Return Credit', '0xd7217f29d51deffc6d5f95ff0a5200f3d34c0f66', '0xd205b3ed8408afca53315798b891f37bd4c5ce2a', null, 'USDC', 6, 18378912),
        ('0x00e0c1ea2085e30e5233e98cfa940ca8cbb1b0b7', 'M11 Credit USDC1', '0x6b6491aaa92ce7e901330d8f91ec99c2a157ebd7', null, '0xcc8058526de295c6ad917cb41416366d69a24cde', 'USDC', 6, 12428576),
        ('0xd3cd37a7299b963bbc69592e5ba933388f70dc88', 'M11 Credit USDC2', '0x74cb3c1938a15e532cc1b465e3b641c2c7e40c2b', null, '0x6f6c8013f639979c84b756c7fc1500eb5af18dc4', 'USDC', 6, 12428576),
        ('0xd2b01f8327eeca47829efc731f1a89c6d07e6b92', 'M11 Credit USDC3', '0x9b300a28d7dc7d422c7d1b9442db0b51a6346e00', null, null, 'USDC', 6, 12428576),
        ('0xfff9a1caf78b2e5b0a49355a8637ea78b43fb6c3', 'M11 Credit WETH', '0x373bdcf21f6a939713d5de94096ffdb24a406391', null, '0x1a066b0109545455bc771e49e6edef6303cb0a93', 'WETH', 18, 12428576),
        ('0x79400a2c9a5e2431419cac98bf46893c86e8bdd7', 'Orthogonal Credit USDC1', '0xfdc7541201aa6831a64f96582111ced633fa5078', null, '0xfebd6f15df3b73dc4307b1d7e65d46413e710c27', 'USDC', 6, 12428576),
        ('0x3e701d29fcb8747b5c3f88649397d88fff9bd3e9', 'Alameda Research - USDC', null, null, '0x3e701d29fcb8747b5c3f88649397d88fff9bd3e9', 'USDC', 6, 12428576),
        ('0xa1fe1b5fc23c2dab0c28d4cc09021014f30be8f1', 'Celsius WETH Pool', null, null, '0xa1fe1b5fc23c2dab0c28d4cc09021014f30be8f1', 'WETH', 18, 12428576),
        ('0xd618d93676762a8e3107554d9adbff7dfd7fbf47', 'BlockTower Capital - USDC01', null, null, '0xd618d93676762a8e3107554d9adbff7dfd7fbf47', 'USDC', 6, 12428576)
) as pools (pool_id, pool_name, loan_manager, open_term_loan_manager, v1_pool_id, asset, precision, block_activated)
