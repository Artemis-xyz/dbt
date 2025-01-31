{{
    config(
        materialized="table",
        snowflake_warehouse="MANTLE",
    )
}}

{{ get_treasury_balance(
        chain = 'ethereum',
        addresses = [
            '0x78605Df79524164911C144801f41e9811B7DB73D',
            '0x5c128d25a21f681e678cb050e551a895c9309945',
            '0xe5791f93b997c7fc90753a1f2711e479773a2a87',
            '0xf6032c7c15bf4b56bfc5d69208f9ce47f5958512',
            '0xb67e28a7e0d1ad886eeeb18b0bda55b7efb56113',
            '0x991a91681f80cb890338b89c1a72be719a902d8b',
            '0x16fe6e64447051b1eb68d6408f041ac22f6fd563',
            '0x44b4babd7cbc8ce32dc3ff77ed9b6df9e2d11003',
            '0x3329fbcda16f15c4ed1d6847bf18e9d045ee941f',
            '0xa2e5e8a607562b7bda05d5820e569c290b43be6d',
            '0x3a9b1da81ca44febc97a713242f6a3feeec7c891',
            '0xc20c13d2303eeaeeaeb7f73babf7014bce6d130a',
            '0x1b9cef6bdd029f378c511e5e6c20ee556b6781b9'
        ],
        earliest_date = '2023-06-25'
    )
}}