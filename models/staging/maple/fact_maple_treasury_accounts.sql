{{
    config( 
        materialized="incremental",
        snowflake_warehouse="MAPLE",
        unique_key="address"
    )
}}


with all_treasury_accounts as (
    select address as treasury_account
    from (
        values
            ('0xa7cC8d3E64EA81670181B005A476D0cA46E4C1fc'),
            ('0xd6d4Bcde6c816F17889f1Dd3000aF0261B03a196'),
            ('0x6a01C16EB312B80535F4799E4BF7522B715AAcfF'),
            ('0x9c9499edD0cd2dCBc3C9Dd5070bAf54777AD8F2C')
    ) as t(address)
)

SELECT 
    DISTINCT treasury_account as address
    , NULL as name
    , 'maple' as artemis_application_id
    , 'ethereum' as chain
    , NULL as is_token
    , NULL as is_fungible
    , 'treasury' as type
    , SYSDATE()::TIMESTAMP_NTZ as last_updated
FROM all_treasury_accounts