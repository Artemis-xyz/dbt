{{
    config(
        materialized="table",
        snowflake_warehouse="GAINS_NETWORK"
    )
}}

with arbitrum as (
    {{get_treasury_balance(
        chain='arbitrum',
        addresses=['0xd85E038593d7A098614721EaE955EC2022B9B91B', 
                  '0xd3443ee1e91aF28e5FB858Fbd0D72A63bA8046E0',
                  '0x5977A9682D7AF81D347CFc338c61692163a2784C',
                  '0xFF162c694eAA571f685030649814282eA457f169'],
        earliest_date='2022-01-01'
    )}}
),

polygon as (
    {{get_treasury_balance(
        chain='polygon',
        addresses=['0x91993f2101cc758D0dEB7279d41e880F7dEFe827',
                  '0x29019Fe2e72E8d4D2118E8D0318BeF389ffe2C81',
                  '0x1544E1fF1a6f6Bdbfb901622C12bb352a43464Fb',
                  '0x209A9A01980377916851af2cA075C2b170452018'],
        earliest_date='2022-01-01'
    )}}
),

base as (
    {{get_treasury_balance(
        chain='base',
        addresses=['0xad20523A7dC37bAbc1CC74897E4977232b3D02e5',
                  '0x6cD5aC19a07518A8092eEFfDA4f1174C72704eeb'],
        earliest_date='2023-08-01'
    )}}
)

select * from arbitrum
union all
select * from polygon
union all
select * from base