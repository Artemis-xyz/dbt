{{config(materialized="incremental", unique_key=["date"], snowflake_warehouse="MENTO")}}

with
    contract_addresses_mento as (
        select contract_address
        from (
            values
            ('0x765de816845861e75a25fca122bb6898b8b1282a')
            , ('0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73')
            , ('0xe8537a3d056da446677b9e9d6c5db704eaab4787')
            , ('0x73F93dcc49cB8A239e2032663e9475dd5ef29A08')
            , ('0x456a3D042C0DbD3db53D5489e98dFb038553B0d0')
            , ('0x105d4A9306D2E55a71d2Eb95B81553AE1dC20d7B')
            , ('0x8a567e2ae79ca692bd748ab832081c45de4041ea')
            , ('0xfAeA5F3404bbA20D3cc2f8C4B0A888F55a3c7313')
            , ('0xCCF663b1fF11028f0b19058d0f7B674004a40746')
            , ('0x4c35853A3B4e647fD266f4de678dCc8fEC410BF6')
            , ('0xff4Ab19391af240c311c54200a492233052B6325')
            , ('0x7175504C455076F15c04A2F90a8e352281F492F9')
            , ('0xb55a79F398E759E43C95b979163f30eC87Ee131D')
            , ('0xc45eCF20f3CD864B32D9794d6f76814aE8892e20')
            , ('0xE2702Bd97ee33c88c8f6f92DA3B733608aa76F71')
        )t(contract_address)
    )
    , stablecoin_metrics_mento as (
        select
            date
            , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
            , sum(stablecoin_daily_txns) as stablecoin_daily_txns
            , sum(stablecoin_supply) as stablecoin_supply
            , sum(p2p_stablecoin_transfer_volume) as p2p_stablecoin_transfer_volume
            , sum(p2p_stablecoin_daily_txns) as p2p_stablecoin_daily_txns
            , sum(artemis_stablecoin_transfer_volume) as artemis_stablecoin_transfer_volume
            , sum(artemis_stablecoin_daily_txns) as artemis_stablecoin_daily_txns
        from {{ref("ez_celo_stablecoin_metrics_by_address_with_labels")}}
        where lower(contract_address) in (select lower(contract_address) from contract_addresses_mento)
            {% if is_incremental() %}
                and date >= (select DATEADD('day', -3, max(date)) from {{ this }})
            {% endif %}
        group by 1
    )
select
    date
    , stablecoin_transfer_volume
    , stablecoin_daily_txns
    , stablecoin_supply
    , p2p_stablecoin_transfer_volume
    , p2p_stablecoin_daily_txns
    , artemis_stablecoin_transfer_volume
    , artemis_stablecoin_daily_txns
from stablecoin_metrics_mento
where date < to_date(sysdate())