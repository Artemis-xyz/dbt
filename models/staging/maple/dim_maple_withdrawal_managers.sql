{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
    )
}}


SELECT 
    * 
FROM 
    ( VALUES 
        ('0x8a665131e796203a5232527fac441480e02fbb7f', 'High Yield Secured Lending', '0xc39a5a616f0ad1ff45077fa2de3f79ab8eb8b8b9'),
        ('0x1bc47a0dd0fdab96e9ef982fdf1f34dc6207cfe3', 'Syrup USDC', '0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b'),
        ('0x86ebdf902d800f2a82038290b6dbb2a5ee29eb8c', 'Syrup USDT', '0x356b8d89c1e1239cbbb9de4815c39a1474d5ba7d'),
        ('0xf18066db3a9590c401e1841598ad90663b4c6d23', 'Secured Lending', '0xc1dd3f011290f212227170f0d02f511ebf57e433'),
        ('0xb7ae6358aba6e7a60c7b921b8cbb3fddb3ee9060', 'Secured Lending', '0xc1dd3f011290f212227170f0d02f511ebf57e433'),
        ('0xeb7b1e9c750190214cdfbbaf0abe398a5e47d230', 'High Yield Corporate USDC', '0x6174a27160f4d7885db4ffed1c0b5fbd66c87f3a'),
        ('0x58a534945f357aa0d2fb56b8bdf7dfa1073bd7a1', 'High Yield Corporate WETH', '0xccbc525ed9d85ad8325b7b6c4c6a79f5566dea3b'),
        ('0x447dcea1d616f792645ed6e71bc32955a0dbcbaa', 'Maple Cash USDC1', '0xfe119e9c24ab79f1bdd5dd884b86ceea2ee75d92'),
        ('0x1146691782c089bcf0b19acb8620943a35eebd12', 'Maple Cash USDC1', '0xfe119e9c24ab79f1bdd5dd884b86ceea2ee75d92'),
        ('0xf0a66f70064ad3198abb35aae26b1eeeaea62c4b', 'Maple Cash USDT1', '0xf05681a33a9adf14076990789a89ab3da3f6b536'),
        ('0x515f77fc8e1473591a89181a2cf6cd0aaf3f932d', 'AQRU Receivables Financing', '0xe9d33286f0e37f517b1204aa6da085564414996d'),
        ('0x8228719ea6dcc79b77d663f13af98684a637d3a0', 'AQRU Receivables Financing', '0xe9d33286f0e37f517b1204aa6da085564414996d'),
        ('0x1b56856eb74bb1aa9e9f1997386ddb28def532ee', 'M11 Credit USDC1', '0x00e0c1ea2085e30e5233e98cfa940ca8cbb1b0b7'),
        ('0x7ed195a0ae212d265511b0978af577f59876c9bb', 'M11 Credit USDC2', '0xd3cd37a7299b963bbc69592e5ba933388f70dc88'),
        ('0x7f0d63e2250bc99f48985b183af0c9a66bbc8ac3', 'M11 Credit USDC3', '0xd2b01f8327eeca47829efc731f1a89c6d07e6b92'),
        ('0x1bb73d6384ae73da2101a4556a42eab82803ef3d', 'M11 Credit WETH', '0xfff9a1caf78b2e5b0a49355a8637ea78b43fb6c3'),
        ('0xd8f8bd488ba6ddf2a710f6c357a884fd1706981a', 'Orthogonal Credit USDC1', '0x79400a2c9a5e2431419cac98bf46893c86e8bdd7')
) as managers (withdrawal_manager, pool_name, pool_id)