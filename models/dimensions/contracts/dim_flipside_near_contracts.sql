{{ config(materialized="table") }}

with
    deployed_contracts as (
        select ea.tx_signer as address,
        max(ft.modified_timestamp) as modified_timestamp
        from near_flipside.core.ez_actions ea
        join
            near_flipside.core.fact_transactions ft
            on ea.tx_hash
            = ft.tx_hash
        where ea.action_name = 'DeployContract' and ft.tx_receiver = ft.tx_signer
        group by 1
    ),
    contracts_tagged as (
        select
            coalesce(labels.address, contracts.address) as address,
            address_name as name,
            lower(replace(replace(project_name, ' ', '_'), '-', '_')) as namespace,
            case when label_type = 'dex' then 'DEX' else null end as sub_category,
            case
                when label_type = 'cex'
                then 'CeFi'
                when label_type = 'dex'
                then 'DeFi'
                when label_type = 'games'
                then 'Gaming'
                when label_type = 'token'
                then 'Token'
                when label_type = 'defi'
                then 'DeFi'
                when label_type = 'layer2'
                then 'Layer 2'
                when label_type = 'nft'
                then 'NFT'
                when label_type = 'bridge'
                then 'Bridge'
                else null
            end as category,
            contracts.modified_timestamp
        from near_flipside.core.dim_address_labels as labels
        full join deployed_contracts as contracts on labels.address = contracts.address
    )
select
    address,
    name,
    case
        -- imported labels
        when
            address in (
                'token.sweat',
                'tge-lockup.sweat',
                'e589457354361489a89039b8be6737cbc2db4d13919b6ccf23889a60f3b0d8f3',
                'ccb91e1db61e8d7e1d4ae3e043001140132959a86ee35a548b6563a46284a6ea',
                'staking-pool.sweatmint.near'
            )
        then 'sweat'
        when address = 'embr.playember_reserve.near'
        then 'playember'
        when address like '%fewandfar.near'
        then 'few_and_far'
        when
            address
            in ('tickets.stlb.near', 'collectables.stlb.near', 'token.stlb.near')
        then 'seatlab'
        when address in ('social.near')
        then 'near_social'
        when address in ('app.nearcrowd.near', 'mdteam.near')
        then 'nearcrowd'
        when
            address in (
                'login.learnclub.near',
                'referralnft.learnclub.near',
                'redeem.learnclub.near',
                'referral.learnclub.near',
                'learnernft.learnclub.near'
            )
        then 'learnnear'
        when
            address in (
                'boostfarm.ref-labs.near',
                'token.v2.ref-finance.near',
                'v2.ref-finance.near',
                'xtoken.ref-finance.near',
                'v2.ref-farming.near',
                'dclv2.ref-labs.near'
            )
        then 'ref'
        when
            address in (
                'marketplace.paras.near',
                'x.paras.near',
                'token.paras.near',
                'staking.paras.near'
            )
        then 'paras'
        when address like '%.factory.bridge.near'
        then 'rainbow_bridge'
        when address in ('wrap.near')
        then 'wnear'
        when
            address in (
                'spot.spin-fi.near', 'v2_0_2.perp.spin-fi.near', 'v1.vault.spin-fi.near'
            )
        then 'spin'
        when address in ('asset-manager.orderly-network.near')
        then 'orderly_network'
        when
            address in (
                'jumptoken.jumpfinance.near',
                'jumphunter.near',
                'nftstaking.jumpfinance.near',
                'lockedjumptoken.jumpfinance.near',
                'xjumptoken.jumpfinance.near',
                'launchpad.jumpfinance.near'
            )
        then 'jump_finance'
        when address in ('canvas-war.feiyu.near')
        then 'feiyu'
        when address in ('token.v1.realisnetwork.near')
        then 'lis'
        when address in ('ftv2.nekotoken.near', 'stake.nekotoken.near')
        then 'neko'
        when address in ('meta-pool.near', 'meta-token.near')
        then 'meta_pool'
        when address in ('storage.herewallet.near', 'nft.herewallet.near')
        then 'here_wallet'
        when address in ('contract.main.burrow.near', 'token.burrow.near')
        then 'burrow'
        when address in ('v2-nearx.stader-labs.near')
        then 'stader'
        when address in ('linear-protocol.near')
        then 'linear_protocol'
        when address = 'mintbus.near' or address like '%mintbase%'
        then 'mintbase'
        when
            address
            in ('token.pembrock.near', 'v1.pembrock.near', 'rewards.v1.pembrock.near')
        then 'pem'
        when
            address in (
                'fusotao.octopus-registry.near',
                'wat-faucet.fusotao.octopus-registry.near'
            )
        then 'fusotao'
        when address like '%zomland.near%'
        then 'zomland'
        when address in ('%.playible.near')
        then 'playible'
        when address in ('app.l2e.near', 'gold.l2e.near')
        then 'land_to_empire'
        when address in ('usdt.tether-token.near')
        then 'tether'
        when address in ('devgovgigs.near')
        then 'devgovgigs'
        when address in ('citizen.bodega-lab.near')
        then 'bodega_lab'
        when address in ('bearversegame_wallet.near')
        then 'beavers'
        when
            address
            in ('app-rewards.lazyfi-wallet.near.near', 'app-rewards.lazyfi-wallet.near')
        then 'lazyfi'
        when address in ('market.tradeport.near')
        then 'tradeport'
        when address in ('bvrs_the_last_winter.near')
        then 'bearverse'
        when address in ('token.cheddar.near')
        then 'cheddar'
        when address in ('pixeltoken.near')
        then 'pxt'
        when
            address
            in ('artex.marbledao.near', 'dex.marbledex.near', 'farming.marbledex.near')
        then 'marble_dao'
        when address in ('phoenix-bonds.near')
        then 'pnear'
        when address in ('token.dangelfund.near')
        then 'dangel'
        when address in ('asac.marketplaces.near')
        then 'asac'
        when address in ('token.elcafecartel.near', 'staking.elcafecartel.near')
        then 'el_cafe_cartel'
        when address in ('launchpad.bocachica_mars.near')
        then 'boca_chica'
        when address in ('megapont.marketplaces.near')
        then 'megapont'
        when address in ('bitverse_games.near')
        then 'bitverse'
        when
            address in (
                'pack_minter.basketball.playible.near',
                'game.basketball.playible.near',
                'athlete.promotional.basketball.playible.near',
                'pack.promotional.basketball.playible.near',
                'open_pack.promotional.baskbetball.playible.near',
                'pack_minter.playible.near',
                'open_pack.nfl.playible.near',
                'pack.pack_minter.playible.near',
                'athlete.nfl.playible.near',
                'game.nfl.playible.near',
                'athlete.promotional.nfl.playible.near',
                'pack.promotional.nfl.playible.near',
                'open_pack.promotional.nfl.playible.near'
            )
        then 'playible'
        when address = 'raigarh_x_airchains.near'
        then 'forest_aadhar'
        when address = 'sankalp_taru_zk.near'
        then 'sankalpataru'
        when address = 'zupple_hpcl.near'
        then 'hpcl'
        when
            address
            in ('thelittles.near', 'stars.thelittles.near', 'prizes.thelittles.near')
        then 'the_littles'
        when address in ('dock-sailgp.near', 'nft.dock-sailgp.near')
        then 'sailgp'
        when address in ('clashrowtoken.near', 'nft.clashrow.near')
        then 'hamthor'
        when address = 'jungly-smartcontract-nft.near'
        then 'nestle_jungly'
        when address = 'dropt.wallets.fewandfar.near'
        then 'dropt'
        when address = 'stacksports.near'
        then 'stacksports'
        when address = 'rownd.near'
        then 'rownd'
        when address = 'arterra.near'
        then 'arterra'
        when address in ('main.arkana.near', 'u.arkana.near')
        then 'arkana_by_paras'
        when address in ('fractal.i-am-human.near', 'registry.i-am-human.near')
        then 'i_am_human'
        when
            address in (
                'dragon.recurforever.near',
                'recur.recurforever.near',
                'papaya.recurforever.near',
                'pear.recurforever.near',
                'mango.recurforever.near',
                'apple.recurforever.near',
                'cherry.recurforever.near',
                'cherry.recurforever.near'
            )
        then 'recur'
        when address in ('wallet.kaiching', 'earn.kaiching', 'hotwallet.kaiching')
        then 'kaiching'
        when address in ('earn.preprod-kaiching.near', 'wallet.preprod-kaiching.near')
        then 'cosmose_ai'
        when address like '%nearapac%'
        then 'nearapac'
        when address like '%hideyourcash%'
        then 'hideyourcash'
        when address = 'starbox.herewallet.near'
        then 'here_wallet'
        when address = 'app.coinflow.near'
        then 'coinflow'
        when
            address = '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1'
        then 'circle'
        when address = 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near'
        then 'circle'
        when address = 'anthropocene.seventhage.near'
        then 'seventh_age'
        when
            address
            in ('elections.ndc-gwg.near', 'nominations.ndc-gwg.near', 'devgovgigs')
        then 'ndc'
        when address = 'fusotao-token.near'
        then 'fusotao'
        when address like '%jumbo_exchange%'
        then 'jumbo_exchange'
        else namespace
    end as namespace,
    sub_category,
    category,
    modified_timestamp as last_updated
from contracts_tagged
