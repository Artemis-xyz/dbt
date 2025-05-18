WITH
     __dbt__cte__dim_chart_of_accounts as (


SELECT code, primary_label, secondary_label, account_label, category_label, subcategory_label
    FROM (VALUES
        (11110, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'ETH', 'ETH'),
        (11120, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'BTC', 'BTC'),
        (11130, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'WSTETH', 'WSTETH'),
        (11140, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Liquidity Pool', 'Stable LP'),
        (11141, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Liquidity Pool', 'Volatile LP'),
        (11199, 'Assets', 'Collateralized Lending', 'Crypto-Loans', 'Other', 'Other'),
        (11210, 'Assets', 'Collateralized Lending', 'Money Market', 'Money Market', 'D3M'),
        (11510, 'Assets', 'Collateralized Lending', 'Legacy', 'Stablecoins', 'Stablecoins'),
        (12310, 'Assets', 'Real-World Lending', 'RWA', 'Private Credit RWA', 'Off-Chain Private Credit'),
        (12311, 'Assets', 'Real-World Lending', 'RWA', 'Private Credit RWA', 'Tokenized Private Credit'),
        (12320, 'Assets', 'Real-World Lending', 'RWA', 'Public Credit RWA', 'Off-Chain Public Credit'),
        (12321, 'Assets', 'Real-World Lending', 'RWA', 'Public Credit RWA', 'Tokenized Public Credit'),
        (13410, 'Assets', 'Liquidity Pool', 'PSM', 'PSM', 'Non-Yielding Stablecoin'),
        (13411, 'Assets', 'Liquidity Pool', 'PSM', 'PSM', 'Yielding Stablecoin'),
        (14620, 'Assets', 'Proprietary Treasury', 'Holdings', 'Treasury Assets', 'DS Pause Proxy'),
        (19999, 'Assets', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token'),
        (21110, 'Liabilities', 'Stablecoin', 'Circulating', 'Interest-bearing', 'Dai'),
        (21120, 'Liabilities', 'Stablecoin', 'Circulating', 'Non-interest bearing', 'Dai'),
        (29999, 'Liabilities', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token'),
        (31110, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'ETH', 'ETH SF'),
        (31120, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'BTC', 'BTC SF'),
        (31130, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'WSTETH', 'WSTETH SF'),
        (31140, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Liquidity Pool', 'Stable LP SF'),
        (31141, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Liquidity Pool', 'Volatile LP SF'),
        (31150, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Other', 'Other SF'),
        (31160, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Money Market', 'D3M SF'),
        (31170, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Off-Chain Private Credit SF'),
        (31171, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Tokenized Private Credit SF'),
        (31172, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Off-Chain Public Credit Interest'),
        (31173, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'RWA', 'Tokenized Public Credit Interest'),
        (31180, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'PSM', 'Yielding Stablecoin Interest'),
        (31190, 'Equity', 'Protocol Surplus', 'Gross Interest Revenues', 'Stablecoins', 'Stablecoins SF'),
        (31210, 'Equity', 'Protocol Surplus', 'Liquidation Revenues', 'Liquidation Revenues', 'Liquidation Revenues'),
        (31310, 'Equity', 'Protocol Surplus', 'Trading Revenues', 'Trading Revenues', 'Trading Revenues'),
        (31311, 'Equity', 'Protocol Surplus', 'Trading Revenues', 'Trading Revenues', 'Teleport Revenues'),
        (31410, 'Equity', 'Protocol Surplus', 'MKR Mints Burns', 'MKR Mints', 'MKR Mints'),
        (31420, 'Equity', 'Protocol Surplus', 'MKR Mints Burns', 'MKR Burns', 'MKR Burns'),
        (31510, 'Equity', 'Protocol Surplus', 'Sin', 'Sin Inflow', 'Sin Inflow'),
        (31520, 'Equity', 'Protocol Surplus', 'Sin', 'Sin Outflow', 'Sin Outflow'),
        (31610, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'DSR', 'Circulating Dai'),
        (31620, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'Liquidation Expenses', 'Liquidation Expenses'),
        (31630, 'Equity', 'Protocol Surplus', 'Direct Expenses', 'Oracle Gas Expenses', 'Oracle Gas Expenses'),
        (31710, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Keeper Maintenance', 'Keeper Maintenance'),
        (31720, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Workforce Expenses', 'Workforce Expenses'),
        (31730, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Workforce Expenses', 'Returned Workforce Expenses'),
        (31740, 'Equity', 'Protocol Surplus', 'Indirect Expenses', 'Direct to Third Party Expenses', 'Direct to Third Party Expenses'),
        (32110, 'Equity', 'Reserved MKR Surplus', 'MKR Token Expenses', 'Direct MKR Token Expenses', 'Direct MKR Token Expenses'),
        (32120, 'Equity', 'Reserved MKR Surplus', 'MKR Token Expenses', 'Vested MKR Token Expenses', 'Vested MKR Token Expenses'),
        (32210, 'Equity', 'Reserved MKR Surplus', 'MKR Contra Equity', 'MKR Contra Equity', 'MKR Contra Equity'),
        (33110, 'Equity', 'Proprietary Treasury', 'Holdings', 'Treasury Assets', 'DS Pause Proxy'),
        (39999, 'Equity', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token', 'Currency Translation to Presentation Token')

    ) AS t(code, primary_label, secondary_label, account_label, category_label, subcategory_label)
),  __dbt__cte__fact_dai_burn as (


SELECT
    block_timestamp,
    tx_hash,
    from_address as usr
FROM ethereum_flipside.core.ez_token_transfers
where to_address = '0x0000000000000000000000000000000000000000'
and lower(contract_address) = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
),  __dbt__cte__dim_dao_wallet as (


SELECT * FROM (VALUES
        ( '0x9e1585d9ca64243ce43d42f7dd7333190f66ca09' , 'RWF Core Unit Multisig + Operational 1', 'Fixed', 'RWF-001')
        , ( '0xd1505ee500791490de8642353ba6a5b92e3550f7' , 'RWF Core Unit Multisig + Operational 2', 'Fixed', 'RWF-001')
        , ( '0xe2c16c308b843ed02b09156388cb240ced58c01c' , 'PE Core Unit Multisig + PE Continuous Ops Multisig 1', 'Fixed', 'PE-001')
        , ( '0x83e36aaa1c7b99e2d3d07789f7b70fce46f0d45e' , 'PE Core Unit Multisig + PE Continuous Ops Multisig 2', 'Fixed', 'PE-001')
        , ( '0x01d26f8c5cc009868a4bf66e268c17b057ff7a73' , 'GovAlpha Multisig', 'Fixed', 'GOV-001')
        , ( '0xdcaf2c84e1154c8ddd3203880e5db965bff09b60' , 'Content Prod Multisig 1', 'Fixed', 'OLD-001')
        , ( '0x6a0ce7dbb43fe537e3fd0be12dc1882393895237' , 'Content Prod Multisig 2', 'Fixed', 'OLD-001')
        , ( '0x1ee3eca7aef17d1e74ed7c447ccba61ac76adba9' , 'GovCom Multisig + Continuous Operation 1', 'Fixed', 'COM-001')
        , ( '0x99e1696a680c0d9f426be20400e468089e7fdb0f' , 'GovCom Multisig + Continuous Operation 2', 'Fixed', 'COM-001')
        , ( '0x7800c137a645c07132886539217ce192b9f0528e' , 'Growth Emergency Multisig', 'Fixed', 'GRO-001')
        , ( '0xb5eb779ce300024edb3df9b6c007e312584f6f4f' , 'SES Multisigs (Permanent Team, Incubation, Grants) 1', 'Fixed', 'SES-001')
        , ( '0x7c09ff9b59baaebfd721cbda3676826aa6d7bae8' , 'SES Multisigs (Permanent Team, Incubation, Grants) 2', 'Fixed', 'SES-001')
        , ( '0xf95eb8ec63d6059ba62b0a8a7f843c7d92f41de2' , 'SES Multisigs (Permanent Team, Incubation, Grants) 3', 'Fixed', 'SES-001')
        , ( '0xd98ef20520048a35eda9a202137847a62120d2d9' , 'Risk Multisig', 'Fixed', 'RISK-001')
        , ( '0x8cd0ad5c55498aacb72b6689e1da5a284c69c0c7' , 'DUX Team Wallet', 'Fixed', 'DUX-001')
        , ( '0x6d348f18c88d45243705d4fdeeb6538c6a9191f1' , 'StarkNet Team Wallet', 'Fixed', 'SNE-001')
        , ( '0x955993df48b0458a01cfb5fd7df5f5dca6443550' , 'Strategic Happiness Wallet 1', 'Fixed', 'SH-001') --prior primary wallet, still uses for smaller payments
        , ( '0xc657ac882fb2d6ccf521801da39e910f8519508d' , 'Strategic Happiness Wallet 2', 'Fixed', 'SH-001') --multisig for most expenses
        , ( '0xd740882b8616b50d0b317fdff17ec3f4f853f44f' , 'CES Team Wallet', 'Fixed', 'CES-001')
        , ( '0x56349a38e09f36039f6af77309690d217beaf0bf' , 'DECO Ops + DECO Protocol Wallets 1', 'Fixed', 'DECO-001')
        , ( '0xa78f1f5698f8d345a14d7323745c6c56fb8227f0' , 'DECO Ops + DECO Protocol Wallets 2', 'Fixed', 'DECO-001')
        , ( '0x465aa62a82e220b331f5ecca697c20e89554b298' , 'SAS Team Wallet', 'Fixed', 'SAS-001')
        , ( '0x124c759d1084e67b19a206ab85c4527fab26c342' , 'IS Ops Wallet', 'Fixed', 'IS-001')
        , ( '0x7327aed0ddf75391098e8753512d8aec8d740a1f' , 'Data Insights Wallet', 'Fixed', 'DIN-001')
        , ( '0x2dc0420a736d1f40893b9481d8968e4d7424bc0b' , 'TechOps', 'Fixed', 'TECH-001')
        , ( '0x2b6180b413511ce6e3da967ec503b2cc19b78db6' , 'Oracle Gas Cost Multisig + Emergency Fund 1', 'Variable', 'GAS')
        , ( '0x1a5b692029b157df517b7d21a32c8490b8692b0f' , 'Oracle Gas Cost Multisig + Emergency Fund 2', 'Variable', 'GAS')
        , ( '0x53ccaa8e3bef14254041500acc3f1d4edb5b6d24' , 'Oracle Multisig, Emergency Multisig 1', 'Fixed', 'ORA-001')
        , ( '0x2d09b7b95f3f312ba6ddfb77ba6971786c5b50cf' , 'Oracle Multisig, Emergency Multisig 2', 'Fixed', 'ORA-001')
        , ( '0xf737c76d2b358619f7ef696cf3f94548fecec379' , 'Strategic Finance Multisig', 'Fixed', 'SF-001')
        , ( '0x3d274fbac29c92d2f624483495c0113b44dbe7d2' , 'Events Multisig', 'Fixed', 'EVENTS-001')
        , ( '0x34d8d61050ef9d2b48ab00e6dc8a8ca6581c5d63' , 'Foundation Operational Wallet', 'Fixed', 'DAIF-001')
        , ( '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb' , 'DS Pause Proxy', 'Variable', 'DSPP')
        , ( '0x73f09254a81e1f835ee442d1b3262c1f1d7a13ff' , 'Interim Multisig', 'Fixed', 'INTERIM')
        , ( '0x87acdd9208f73bfc9207e1f6f0fde906bca95cc6' , 'SES Multisig (Auditor)', 'Fixed', 'SES-001')
        , ( '0x5a994d8428ccebcc153863ccda9d2be6352f89ad' , 'DUX Auditor Wallet', 'Fixed', 'DUX-001')
        , ( '0x25307ab59cd5d8b4e2c01218262ddf6a89ff86da' , 'CES Auditor Wallet', 'Fixed', 'CES-001')
        , ( '0xf482d1031e5b172d42b2daa1b6e5cbf6519596f7' , 'DECO Auditor Wallet', 'Fixed', 'DECO-001')
        , ( '0xb1f950a51516a697e103aaa69e152d839182f6fe' , 'SAS Auditor Wallet', 'Fixed', 'SAS-001')
        , ( '0xd1f2eef8576736c1eba36920b957cd2af07280f4' , 'IS Auditor Wallet', 'Fixed', 'IS-001')
        , ( '0x96d7b01cc25b141520c717fa369844d34ff116ec' , 'RWF Auditor Wallet', 'Fixed', 'RWF-001')
        , ( '0x1a3da79ee7db30466ca752de6a75def5e635b2f6' , 'TechOps Auditor Wallet', 'Fixed', 'TECH-001')
        , ( '0x5f5c328732c9e52dfcb81067b8ba56459b33921f' , 'Foundation Reserves', 'Fixed', 'DAIF-001')
        , ( '0x478c7ce3e1df09130f8d65a23ad80e05b352af62' , 'Gelato Keepers', 'Variable', 'GELATO')
        , ( '0x926c21602fec84d6d0fa6450b40edba595b5c6e4' , 'Gelato Keepers', 'Variable', 'GELATO')
        , ( '0x37b375e3d418fbecba6b283e704f840ab32f3b3c' , 'Keep3r Keepers', 'Variable', 'KEEP3R')
        , ( '0x5a6007d17302238d63ab21407ff600a67765f982' , 'Techops Keepers', 'Variable', 'TECHOPS')
        , ( '0xfb5e1d841bda584af789bdfabe3c6419140ec065' , 'Chainlink Keepers', 'Variable', 'CHAINLINK')
        , ( '0xaefed819b6657b3960a8515863abe0529dfc444a' , 'Keep3r Keepers', 'Variable', 'KEEP3R')
        , ( '0x0b5a34d084b6a5ae4361de033d1e6255623b41ed' , 'Gelato Keepers', 'Variable', 'GELATO')
        , ( '0xc6a048550c9553f8ac20fbdeb06f114c27eccabb' , 'Gelato Keepers', 'Variable', 'GELATO')
        --, ( '0x0048fc4357db3c0f45adea433a07a20769ddb0cf' , 'DSS Blow', 'Variable', 'BLOW')
        , ( '0xb386bc4e8bae87c3f67ae94da36f385c100a370a' , 'New Risk Multisig', 'Fixed', 'RISK-001')
    ) AS  t(wallet_address, wallet_label, varfix, code)
),  __dbt__cte__fact_team_dai_burns_tx as (


SELECT 
    tx_hash,
    usr,
    is_keeper
FROM (
    SELECT 
        d_c_b.tx_hash,
        d_c_b.usr,
        dao_wallet.wallet_label LIKE '% Keepers' as is_keeper
    FROM __dbt__cte__fact_dai_burn as d_c_b
    JOIN __dbt__cte__dim_dao_wallet dao_wallet ON d_c_b.usr = dao_wallet.wallet_address

    UNION ALL

    SELECT 
        tx_hash,
        usr,
        FALSE as is_keeper
    FROM __dbt__cte__fact_dai_burn
    WHERE usr = '0x0048fc4357db3c0f45adea433a07a20769ddb0cf'
)
GROUP BY tx_hash, usr, is_keeper
),  __dbt__cte__fact_team_dai_burns as (


WITH team_dai_burns_preunioned AS (
    SELECT vat.block_timestamp AS ts,
           vat.tx_hash AS hash,
           tx.is_keeper,
           SUM(vat.rad) AS value
    FROM ethereum_flipside.maker.fact_vat_move vat
    JOIN __dbt__cte__fact_team_dai_burns_tx tx
        ON vat.tx_hash = tx.tx_hash
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
    GROUP BY vat.block_timestamp, vat.tx_hash, tx.is_keeper
)

SELECT ts,
       hash,
       (CASE WHEN is_keeper THEN 31710 ELSE 31730 END) AS code,
       value -- increased equity
FROM team_dai_burns_preunioned

UNION ALL

SELECT ts,
       hash,
       21120 AS code,
       -value AS value -- decreased liability
FROM team_dai_burns_preunioned
),  __dbt__cte__dim_maker_contracts as (


SELECT * FROM (VALUES
        ( 'FlapFlop', '0x4d95a049d5b0b7d32058cd3f2163015747522e99' )
        , ( 'FlapFlop', '0xa4f79bc4a5612bdda35904fdf55fc4cb53d1bff6' )
        , ( 'FlapFlop', '0x0c10ae443ccb4604435ba63da80ccc63311615bc' )
        , ( 'FlapFlop', '0xa41b6ef151e06da0e34b009b86e828308986736d' )
        , ( 'FlapFlop', '0xc4269cc7acdedc3794b221aa4d9205f564e27f0d' )
        , ( 'PSM', '0x961ae24a1ceba861d1fdf723794f6024dc5485cf' )
        , ( 'PSM', '0x204659b2fd2ad5723975c362ce2230fba11d3900' )
        , ( 'PSM', '0x89b78cfa322f6c5de0abceecab66aee45393cc5a' )
    ) AS t(contract_type, contract_address)
),  __dbt__cte__fact_liquidation_excluded_tx as (


SELECT DISTINCT t.tx_hash
FROM ethereum_flipside.core.fact_traces t
JOIN __dbt__cte__dim_maker_contracts c
    ON t.from_address = c.contract_address
    AND c.contract_type IN ('FlapFlop')
),  __dbt__cte__fact_psm_yield_tx as (


SELECT DISTINCT
    tx_hash,
    CASE
        WHEN usr = '0xf2e7a5b83525c3017383deed19bb05fe34a62c27'
        THEN 'PSM-GUSD-A'
        WHEN usr = lower('0x8bF8b5C58bb57Ee9C97D0FEA773eeE042B10a787')
        THEN 'PSM-USDP-A'
    END AS ilk
FROM __dbt__cte__fact_dai_burn
WHERE usr IN ('0xf2e7a5b83525c3017383deed19bb05fe34a62c27', lower('0x8bF8b5C58bb57Ee9C97D0FEA773eeE042B10a787')) -- GUSD interest payment contract
),  __dbt__cte__fact_rwa_yield_tx as (


SELECT DISTINCT
    tx_hash,
    CASE 
        WHEN usr = '0x6c6d4be2223b5d202263515351034861dd9afdb6' THEN 'RWA009-A'
        WHEN usr = '0xef1b095f700be471981aae025f92b03091c3ad47' THEN 'RWA007-A'
        WHEN usr = '0x71ec6d5ee95b12062139311ca1fe8fd698cbe0cf' THEN 'RWA014-A'
        WHEN usr = lower('0xc27C3D3130563C1171feCC4F76C217Db603997cf') THEN 'RWA015-A'
    END AS ilk
FROM __dbt__cte__fact_dai_burn
WHERE usr IN ('0x6c6d4be2223b5d202263515351034861dd9afdb6', '0xef1b095f700be471981aae025f92b03091c3ad47', '0x71ec6d5ee95b12062139311ca1fe8fd698cbe0cf', lower('0xc27C3D3130563C1171feCC4F76C217Db603997cf'))
),  __dbt__cte__fact_liquidation_revenue as (


SELECT 
    block_timestamp AS ts,
    tx_hash AS hash,
    SUM(CAST(rad AS DOUBLE)) AS value
FROM ethereum_flipside.maker.fact_vat_move
WHERE 
    dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'  -- vow
    AND src_address NOT IN (SELECT contract_address FROM __dbt__cte__dim_maker_contracts)
    AND src_address NOT IN (
        '0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a',
        '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2'
    )  -- aave v2 d3m, compound v2 d3m
    AND tx_hash NOT IN (SELECT tx_hash FROM __dbt__cte__fact_liquidation_excluded_tx)
    AND tx_hash NOT IN (SELECT tx_hash FROM __dbt__cte__fact_team_dai_burns_tx)
    AND tx_hash NOT IN (SELECT tx_hash FROM __dbt__cte__fact_psm_yield_tx)
    AND tx_hash NOT IN (SELECT tx_hash FROM __dbt__cte__fact_rwa_yield_tx)
GROUP BY block_timestamp, tx_hash
),  __dbt__cte__fact_vow_fess as (


SELECT
    block_timestamp,
    pc_dbt_db.prod.hex_to_int(topics[2])::double/1e45 as tab,
    tx_hash
FROM ethereum_flipside.core.fact_event_logs
where topics[0] = '0x697efb7800000000000000000000000000000000000000000000000000000000'
and contract_address = lower('0xA950524441892A31ebddF91d3cEEFa04Bf454466')
),  __dbt__cte__fact_liquidation_expenses as (


SELECT block_timestamp
     , tx_hash
     , SUM(tab) AS value
FROM __dbt__cte__fact_vow_fess
GROUP BY block_timestamp
    , tx_hash
),  __dbt__cte__fact_liquidation as (


-- Liquidation Revenues
SELECT
    ts,
    hash,
    31210 AS code,
    value AS value
FROM __dbt__cte__fact_liquidation_revenue

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value
FROM __dbt__cte__fact_liquidation_revenue

UNION ALL

-- Liquidation Expenses
SELECT
    block_timestamp as ts,
    tx_hash as hash,
    31620 AS code,
    -value AS value
FROM __dbt__cte__fact_liquidation_expenses

UNION ALL

SELECT
    block_timestamp as ts,
    tx_hash as hash,
    21120 AS code,
    value AS value
FROM __dbt__cte__fact_liquidation_expenses
),  __dbt__cte__dim_psms as (


SELECT DISTINCT
    u_address as psm_address,
    ilk
FROM ethereum_flipside.maker.fact_vat_frob
WHERE ilk LIKE 'PSM-%'
),  __dbt__cte__fact_trading_revenues as (


WITH trading_revenues_preunion AS (
    SELECT 
        vat.block_timestamp AS ts,
        vat.tx_hash AS hash,
        psms.ilk,
        SUM(CAST(vat.rad AS DOUBLE)) AS value
    FROM ethereum_flipside.maker.fact_vat_move vat
    INNER JOIN __dbt__cte__dim_psms psms
        ON vat.src_address = psms.psm_address
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
    GROUP BY vat.block_timestamp, vat.tx_hash, psms.ilk
)

SELECT
    ts,
    hash,
    31310 AS code,
    value AS value,
    ilk
FROM trading_revenues_preunion

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value,
    ilk
FROM trading_revenues_preunion
),  __dbt__cte__fact_mkr_mints as (


WITH mkr_mints_preunioned AS (
    SELECT 
        vat.block_timestamp AS ts,
        vat.tx_hash AS hash,
        SUM(vat.rad) AS value
    FROM ethereum_flipside.maker.fact_vat_move vat
    JOIN __dbt__cte__fact_liquidation_excluded_tx tx
        ON vat.tx_hash = tx.tx_hash
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
      AND vat.src_address NOT IN (SELECT contract_address FROM __dbt__cte__dim_maker_contracts WHERE contract_type = 'PSM')
    GROUP BY vat.block_timestamp, vat.tx_hash
)

SELECT
    ts,
    hash,
    31410 AS code,
    value --increased equity
FROM mkr_mints_preunioned

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value --decreased liability
FROM mkr_mints_preunioned
),  __dbt__cte__fact_mkr_burns as (


WITH mkr_burns_preunioned AS (
    SELECT
        block_timestamp AS ts,
        tx_hash AS hash,
        SUM(CAST(rad AS DOUBLE)) AS value
    FROM ethereum_flipside.maker.fact_vat_move
    WHERE src_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
    GROUP BY block_timestamp, tx_hash
)

SELECT
    ts,
    hash,
    31420 AS code,
    -value AS value --decreased equity
FROM mkr_burns_preunioned

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    value --increased liability
FROM mkr_burns_preunioned
),  __dbt__cte__fact_vat_grab as (


with logs as (
    SELECT
        *
    FROM
        ethereum_flipside.core.fact_event_logs
    WHERE
        topics [0] in (
            '0x85258d09e1e4ef299ff3fc11e74af99563f022d21f3f940db982229dc2a3358c',
            '0xa716da86bc1fb6d43d1493373f34d7a418b619681cd7b90f7ea667ba1489be28',
            '0x7bab3f4000000000000000000000000000000000000000000000000000000000',
            '0x7cdd3fde00000000000000000000000000000000000000000000000000000000'
        )
),
bark as(
    SELECT
        block_timestamp,
        tx_hash,
        CASE
            WHEN topics [1] LIKE '%4449524543542%' THEN -- Direct modules
            pc_dbt_db.prod.HEX_TO_INT(SUBSTR(data, 67, 64))
            ELSE pc_dbt_db.prod.HEX_TO_INT(SUBSTR(data, 67, 64))::numeric * -1
        END AS dart,
        pc_dbt_db.prod.HEX_TO_UTF8(RTRIM(topics [1], 0)) as ilk
    FROM
        logs
    WHERE
        topics [0] = '0x85258d09e1e4ef299ff3fc11e74af99563f022d21f3f940db982229dc2a3358c'
),
grab as(
    SELECT
        block_timestamp,
        tx_hash,
        pc_dbt_db.prod.HEX_TO_UTF8(rtrim(topics [1], 0)) as ilk
    FROM
        logs
    WHERE
        topics [0] = '0x7bab3f4000000000000000000000000000000000000000000000000000000000'
),
bite as (
    SELECT
        block_timestamp,
        tx_hash,
        CASE
            WHEN topics [1] LIKE '%4449524543542%' THEN -- Direct modules
            pc_dbt_db.prod.HEX_TO_INT(SUBSTR(data, 67, 64))
            ELSE pc_dbt_db.prod.HEX_TO_INT(SUBSTR(data, 67, 64))::numeric * -1
        END AS dart,
        pc_dbt_db.prod.HEX_TO_UTF8(RTRIM(topics [1], 0)) as ilk
    FROM
        logs
    WHERE
        topics [0] = '0xa716da86bc1fb6d43d1493373f34d7a418b619681cd7b90f7ea667ba1489be28'
),
slip_raw as (
    SELECT
        block_timestamp,
        tx_hash,
        CASE
            WHEN topics [1] LIKE '%4449524543542%'
            AND length(pc_dbt_db.prod.HEX_TO_INT(topics [3])) < 50 THEN -- Direct modules
            pc_dbt_db.prod.HEX_TO_INT(topics [3])
            WHEN length(pc_dbt_db.prod.HEX_TO_INT(topics [3])) < 50 THEN pc_dbt_db.prod.HEX_TO_INT(topics [3])::numeric * -1
        END AS dart,
        pc_dbt_db.prod.HEX_TO_UTF8(RTRIM(topics [1], 0)) as ilk
    FROM
        logs
    WHERE
        topics [0] = '0x7cdd3fde00000000000000000000000000000000000000000000000000000000'
),
slip as(
    SELECT
        block_timestamp,
        tx_hash,
        min(dart) as dart,
        -- collision on certain tx where there are two calls to slip()
        ilk
    FROM
        slip_raw
    GROUP BY
        1,
        2,
        4
),
agg as(
    SELECT
        distinct g.block_timestamp,
        g.tx_hash as tx_hash,
        coalesce(b.dart, t.dart, s.dart) as dart,
        g.ilk as ilk
    FROM
        grab g
        LEFT JOIN bark b on b.tx_hash = g.tx_hash
        and g.ilk = b.ilk
        LEFT JOIN bite t on t.tx_hash = g.tx_hash
        and g.ilk = t.ilk
        LEFT JOIN slip s on s.tx_hash = g.tx_hash
        and g.ilk = s.ilk
)
select
    *
from
    agg
where
    dart is not null
),  __dbt__cte__fact_interest_accruals_1 as (


SELECT 
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    dart,
    CAST(NULL AS NUMBER) AS rate
FROM ethereum_flipside.maker.fact_vat_frob
WHERE dart != 0

UNION ALL

SELECT 
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    dart/1e18,
    0 AS rate
FROM __dbt__cte__fact_vat_grab
WHERE dart != 0

UNION ALL

SELECT 
    ilk,
    block_timestamp AS ts,
    tx_hash AS hash,
    CAST(NULL AS NUMBER) AS dart,
    rate
FROM ethereum_flipside.maker.fact_vat_fold
WHERE rate != 0
),  __dbt__cte__fact_interest_accruals_2 as (


SELECT 
    *,
    SUM(dart) OVER (PARTITION BY ilk ORDER BY ts) AS cumulative_dart
FROM __dbt__cte__fact_interest_accruals_1
),  __dbt__cte__fact_interest_accruals_3 as (


SELECT 
    ilk,
    ts,
    hash,
    SUM(cumulative_dart * rate * 10) AS interest_accruals
FROM __dbt__cte__fact_interest_accruals_2
WHERE rate IS NOT NULL
GROUP BY ilk, ts, hash
),  __dbt__cte__dim_ilk_list_manual_input as (


SELECT * FROM (VALUES
    ('RWA009-A', NULL, NULL, 12310, 31170, NULL),
    ('RWA007-A', NULL, NULL, 12310, 31172, NULL),
    ('RWA015-A', NULL, NULL, 12310, 31172, NULL),
    ('RWA014-A', NULL, NULL, 12310, 31180, NULL)
) AS t(ilk, begin_dt, end_dt, asset_code, equity_code, apr)
),  __dbt__cte__fact_spot_file as (



SELECT 
    block_timestamp,
    pc_dbt_db.prod.HEX_TO_UTF8(rtrim(topics[2], 0)) as ilk,
    tx_hash
FROM ethereum_flipside.core.fact_event_logs
where topics[0] = '0x1a0b287e00000000000000000000000000000000000000000000000000000000'
and contract_address ilike '0x65C79fcB50Ca1594B025960e539eD7A9a6D434A3'
),  __dbt__cte__fact_jug_file as (



SELECT 
    block_timestamp,
    pc_dbt_db.prod.HEX_TO_UTF8(rtrim(topics[2],0)) as ilk,
    tx_hash
FROM ethereum_flipside.core.fact_event_logs
where topics[0] = '0x29ae811400000000000000000000000000000000000000000000000000000000'
and contract_address ilike '0x19c0976f590D67707E62397C87829d896Dc0f1F1'
),  __dbt__cte__fact_ilk_list as (


SELECT DISTINCT ilk
FROM (
    SELECT ilk
    FROM ethereum_flipside.maker.fact_vat_frob

    UNION

    SELECT ilk
    FROM __dbt__cte__fact_spot_file

    UNION

    SELECT ilk
    FROM __dbt__cte__fact_jug_file
)
),  __dbt__cte__dim_ilk_list_labeled as (


-- This table should contain your ilk mappings
-- You may need to adjust this based on your specific ilk categorizations
SELECT 
    ilk,
    begin_dt,
    end_dt,
    asset_code,
    equity_code
FROM __dbt__cte__dim_ilk_list_manual_input

UNION ALL

SELECT
    ilk,
    CAST(NULL AS DATE) AS begin_dt,
    CAST(NULL AS DATE) AS end_dt,
    CASE
        WHEN ilk LIKE 'ETH-%' THEN 11110
        WHEN ilk LIKE 'WBTC-%' OR ilk = 'RENBTC-A' THEN 11120
        WHEN ilk LIKE 'WSTETH-%' OR ilk LIKE 'RETH-%' OR ilk = 'CRVV1ETHSTETH-A' THEN 11130
        WHEN ilk LIKE 'GUNI%' THEN 11140
        WHEN ilk LIKE 'UNIV2%' THEN 11141
        WHEN ilk LIKE 'DIRECT%' THEN 11210
        WHEN ilk LIKE 'RWA%' THEN 12310
        WHEN ilk LIKE 'PSM%' THEN 13410
        WHEN ilk IN ('USDC-A','USDC-B', 'USDT-A', 'TUSD-A','GUSD-A') THEN 11510
        ELSE 11199
    END AS asset_code,
    CASE
        WHEN ilk LIKE 'ETH-%' THEN 31110
        WHEN ilk LIKE 'WBTC-%' OR ilk = 'RENBTC-A'  THEN 31120
        WHEN ilk LIKE 'WSTETH-%' OR ilk LIKE 'RETH-%' OR ilk = 'CRVV1ETHSTETH-A' THEN 31130
        WHEN ilk LIKE 'GUNI%' THEN 31140
        WHEN ilk LIKE 'UNIV2%' THEN 31141
        WHEN ilk LIKE 'DIRECT%' THEN 31160
        WHEN ilk LIKE 'RWA%' THEN 31170
        WHEN ilk LIKE 'PSM%' THEN NULL
        WHEN ilk IN ('USDC-A','USDC-B', 'USDT-A', 'TUSD-A','GUSD-A','PAXUSD-A') THEN 31190
        ELSE 31150
    END AS equity_code
FROM __dbt__cte__fact_ilk_list
WHERE ilk NOT IN (SELECT ilk FROM __dbt__cte__dim_ilk_list_manual_input)
AND ilk <> 'TELEPORT-FW-A'
),  __dbt__cte__fact_interest_accruals as (


WITH interest_accruals AS (
    SELECT 
        ia.ts,
        ia.hash,
        il.equity_code AS code,
        SUM(ia.interest_accruals) AS value,
        ia.ilk
    FROM __dbt__cte__fact_interest_accruals_3 ia
    LEFT JOIN __dbt__cte__dim_ilk_list_labeled il
        ON ia.ilk = il.ilk
        AND ia.ts BETWEEN COALESCE(il.begin_dt, '2000-01-01') AND COALESCE(il.end_dt, '2222-12-31')
    GROUP BY ia.ts, ia.hash, il.equity_code, ia.ilk

    UNION ALL

    SELECT 
        ia.ts,
        ia.hash,
        il.asset_code AS code,
        SUM(ia.interest_accruals) AS value,
        ia.ilk
    FROM __dbt__cte__fact_interest_accruals_3 ia
    LEFT JOIN __dbt__cte__dim_ilk_list_labeled il
        ON ia.ilk = il.ilk
        AND CAST(ia.ts AS DATE) BETWEEN COALESCE(il.begin_dt, '2000-01-01') AND COALESCE(il.end_dt, '2222-12-31')
    GROUP BY ia.ts, ia.hash, il.asset_code, ia.ilk
)

SELECT * FROM interest_accruals
),  __dbt__cte__fact_dai_mint as (


SELECT
    block_timestamp,
    tx_hash,
    to_address as usr,
    raw_amount_precise as wad
FROM ethereum_flipside.core.ez_token_transfers
where from_address = '0x0000000000000000000000000000000000000000'
and lower(contract_address) = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
),  __dbt__cte__fact_opex_suck_hashes as (


SELECT suck.tx_hash
FROM ethereum_flipside.maker.fact_vat_suck suck
WHERE suck.u_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'
  AND suck.v_address IN (
    '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb',
    '0x2cc583c0aacdac9e23cb601fda8f1a0c56cdcb71',
    '0xa4c22f0e25c6630b2017979acf1f865e94695c4b'
  )
  AND suck.rad != 0
GROUP BY 1
),  __dbt__cte__fact_opex as (


WITH opex_preunion AS (
    SELECT 
        mints.block_timestamp AS ts,
        mints.tx_hash AS hash,
        CASE
            WHEN dao_wallet.code IN ('GELATO', 'KEEP3R', 'CHAINLINK', 'TECHOPS') THEN 31710 --keeper maintenance expenses
            WHEN dao_wallet.code = 'GAS' THEN 31630 -- oracle gas expenses
            WHEN dao_wallet.code IS NOT NULL THEN 31720 --workforce expenses
            ELSE 31740 --direct opex - when a suck operation is used to directly transfer DAI to a third party
        END AS equity_code,
        mints.wad / POW(10, 18) AS expense
    FROM __dbt__cte__fact_dai_mint mints
    JOIN __dbt__cte__fact_opex_suck_hashes opex
        ON mints.tx_hash = opex.tx_hash
    LEFT JOIN __dbt__cte__dim_dao_wallet dao_wallet
        ON mints.usr = dao_wallet.wallet_address
    LEFT JOIN ethereum_flipside.maker.fact_vat_frob AS frobs
        ON mints.tx_hash = frobs.tx_hash
        AND mints.wad::number/1e18 = frobs.dart
    WHERE frobs.tx_hash IS NULL --filtering out draws from psm that happened in the same tx as expenses
)

SELECT
    ts,
    hash,
    equity_code AS code,
    -CAST(expense AS DOUBLE) AS value --reduced equity
FROM opex_preunion

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    expense AS value --increased liability
FROM opex_preunion
),  __dbt__cte__fact_dsr_expenses as (


WITH dsr_expenses_raw AS (
    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        CAST(rad AS DOUBLE) AS value
    FROM ethereum_flipside.maker.fact_vat_suck
    WHERE u_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
      AND v_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' -- Pot
)

SELECT
    ts,
    hash,
    31610 AS code,
    -value AS value --reduced equity
FROM dsr_expenses_raw

UNION ALL

SELECT
    ts,
    hash,
    21110 AS code,
    value AS value --increased liability
FROM dsr_expenses_raw
),  __dbt__cte__fact_other_sin_outflows as (


WITH other_sin_outflows_raw AS (
    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        CAST(rad AS DOUBLE) AS value
    FROM ethereum_flipside.maker.fact_vat_suck
    WHERE u_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
      AND v_address NOT IN (
        '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7', -- Pot (DSR)
        '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb', -- Pause Proxy
        '0x2cc583c0aacdac9e23cb601fda8f1a0c56cdcb71', -- Old Pause Proxy
        '0xa4c22f0e25c6630b2017979acf1f865e94695c4b'  -- Old Pause Proxy
      )
)

SELECT
    ts,
    hash,
    31520 AS code,
    -value AS value --reduced equity
FROM other_sin_outflows_raw

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    value AS value --increased liability
FROM other_sin_outflows_raw
),  __dbt__cte__fact_sin_inflows as (


WITH sin_inflows_raw AS (
    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        CAST(rad AS DOUBLE) AS value
    FROM ethereum_flipside.maker.fact_vat_suck
    WHERE v_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
)

SELECT
    ts,
    hash,
    31510 AS code,
    value AS value --increased equity
FROM sin_inflows_raw

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value --decreased liability
FROM sin_inflows_raw
),  __dbt__cte__fact_dsr_flows as (


WITH dsr_flows_preunioned AS (
    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        -CAST(rad AS DOUBLE) AS dsr_flow
    FROM ethereum_flipside.maker.fact_vat_move
    WHERE src_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' -- Pot (DSR) contract

    UNION ALL

    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        CAST(rad AS DOUBLE) AS dsr_flow
    FROM ethereum_flipside.maker.fact_vat_move
    WHERE dst_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' -- Pot (DSR) contract
)

SELECT
    ts,
    hash,
    21110 AS code,
    dsr_flow AS value -- positive dsr flow increases interest-bearing dai liability
FROM dsr_flows_preunioned

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -dsr_flow AS value -- positive dsr flow decreases non-interest-bearing dai liability
FROM dsr_flows_preunioned
),  __dbt__cte__dim_treasury_erc20s as (


SELECT * FROM (VALUES
    ('0xc18360217d8f7ab5e7c516566761ea12ce7f9d72', '0xc18360217d8f7ab5e7c516566761ea12ce7f9d72', 18, 'ENS'),
    ('0x4da27a545c0c5b758a6ba100e3a049001de870f5', '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 18, 'stkAAVE'),
    ('0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 18, 'AAVE'),
    ('0xc00e94cb662c3520282e6f5717214004a7f26888', '0xc00e94cb662c3520282e6f5717214004a7f26888', 18, 'COMP')
) AS t(contract_address, price_address, decimals, token)
),  __dbt__cte__fact_treasury_flows as (


WITH treasury_flows_preunioned AS (
    SELECT 
        evt.block_timestamp AS ts,
        evt.tx_hash AS hash,
        t.token,
        SUM(evt.RAW_AMOUNT_PRECISE / POW(10, t.decimals)) AS value
    FROM ethereum_flipside.core.ez_token_transfers evt
    JOIN __dbt__cte__dim_treasury_erc20s t
        ON evt.contract_address = t.contract_address
    WHERE evt.to_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
    GROUP BY evt.block_timestamp, evt.tx_hash, t.token

    UNION ALL

    SELECT 
        evt.block_timestamp AS ts,
        evt.tx_hash AS hash,
        t.token,
        -SUM(evt.RAW_AMOUNT_PRECISE / POW(10, t.decimals)) AS value
    FROM ethereum_flipside.core.ez_token_transfers evt
    JOIN __dbt__cte__dim_treasury_erc20s t
        ON evt.contract_address = t.contract_address
    WHERE evt.from_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
    AND evt.to_address != '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
    GROUP BY evt.block_timestamp, evt.tx_hash, t.token
)

SELECT
    ts,
    hash,
    33110 AS code,
    value, --increased equity
    token
FROM treasury_flows_preunioned

UNION ALL

SELECT
    ts,
    hash,
    14620 AS code,
    value, --increased assets
    token
FROM treasury_flows_preunioned
),  __dbt__cte__fact_loan_actions_2 as (


SELECT 
    ilk,
    ts,
    hash,
    dart,
    COALESCE(POW(10,27) + SUM(rate) OVER(PARTITION BY ilk ORDER BY ts ASC), POW(10,27)) AS rate
FROM __dbt__cte__fact_interest_accruals_1
WHERE ilk != 'TELEPORT-FW-A'
),  __dbt__cte__fact_loan_actions as (



SELECT 
    la.ts,
    la.hash,
    il.asset_code AS code,
    SUM(la.dart) AS value,
    la.ilk
FROM __dbt__cte__fact_loan_actions_2 la
LEFT JOIN __dbt__cte__dim_ilk_list_labeled il
    ON la.ilk = il.ilk
    AND CAST(la.ts AS DATE) BETWEEN COALESCE(il.begin_dt, '2000-01-01') AND COALESCE(il.end_dt, '2222-12-31')
GROUP BY la.ts, la.hash, il.asset_code, la.ilk
-- HAVING SUM(la.dart * la.rate) / POW(10, 45) != 0

UNION ALL

SELECT 
    ts,
    hash,
    21120 AS code,
    SUM(dart) AS value,
    ilk
FROM __dbt__cte__fact_loan_actions_2
GROUP BY ts, hash, ilk
HAVING SUM(dart) != 0
),  __dbt__cte__fact_d3m_revenues as (


WITH d3m_revenues_preunion AS (
    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        CASE
            WHEN src_address = '0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a' THEN 'DIRECT-AAVEV2-DAI'
            WHEN src_address = '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2' THEN 'DIRECT-COMPV2-DAI'
        END AS ilk,
        SUM(CAST(rad AS DOUBLE)) AS value
    FROM ethereum_flipside.maker.fact_vat_move
    WHERE src_address IN (
        '0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a',  -- aave d3m
        '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2'   -- compound v2 d3m
    )
    AND dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'  -- vow
    GROUP BY 1, 2, 3

    UNION ALL

    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        ilk,
        SUM(dart) / 1e18 AS value
    FROM __dbt__cte__fact_vat_grab
    WHERE dart > 0
    GROUP BY 1, 2, 3
)

SELECT
    ts,
    hash,
    31160 AS code,
    value AS value,
    ilk
FROM d3m_revenues_preunion

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value,
    ilk
FROM d3m_revenues_preunion
),  __dbt__cte__fact_psm_yield as (


WITH psm_yield_preunioned AS (
    SELECT 
        vat.block_timestamp AS ts,
        vat.tx_hash AS hash,
        tx.ilk,
        SUM(vat.rad) AS value
    FROM ethereum_flipside.maker.fact_vat_move vat
    INNER JOIN __dbt__cte__fact_psm_yield_tx tx
        ON vat.tx_hash = tx.tx_hash
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
    GROUP BY vat.block_timestamp, vat.tx_hash, tx.ilk
)

SELECT 
    ts,
    hash,
    31180 AS code,
    value, --increased equity
    ilk
FROM psm_yield_preunioned

UNION ALL

SELECT 
    ts,
    hash,
    21120 AS code,
    -value AS value, --decreased liability
    ilk
FROM psm_yield_preunioned
),  __dbt__cte__fact_rwa_yield as (


WITH rwa_yield_preunioned AS (
    SELECT 
        vat.block_timestamp AS ts,
        vat.tx_hash AS hash,
        tx.ilk,
        SUM(vat.rad) AS value
    FROM ethereum_flipside.maker.fact_vat_move vat
    JOIN __dbt__cte__fact_rwa_yield_tx tx
        ON vat.tx_hash = tx.tx_hash
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
    GROUP BY vat.block_timestamp, vat.tx_hash, tx.ilk
)

SELECT 
    ts,
    hash,
    COALESCE(ilm.equity_code, 31170) AS code, --default to off-chain private credit
    value, --increased equity
    rwy.ilk
FROM rwa_yield_preunioned rwy
LEFT JOIN __dbt__cte__dim_ilk_list_manual_input ilm
    USING (ilk)

UNION ALL

SELECT 
    ts,
    hash,
    21120 AS code,
    -value AS value, --decreased liability
    ilk
FROM rwa_yield_preunioned
),  __dbt__cte__fact_dssvesttransferrable_create as (



with raw as (
SELECT
    trace_index,
    trace_address,
    block_timestamp,
    tx_hash,
    SUBSTR(input, 11) as raw_input_data
FROM ethereum_flipside.core.fact_traces
where to_address = lower('0x6D635c8d08a1eA2F1687a5E46b666949c977B7dd')
and left(input, 10) = '0xdb64ff8f'
)
SELECT
    block_timestamp,
    tx_hash,
    '0x' || SUBSTR(raw_input_data, 25, 40) as _usr,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 65, 64)) as _tot,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 129, 64)) as _bgn,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 193, 64)) as _tau,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 257, 64)) as _eta,
    ROW_NUMBER() OVER (ORDER BY block_timestamp, trace_index) AS output_id
FROM raw
),  __dbt__cte__fact_mkr_vest_creates as (


SELECT
    block_timestamp AS ts,
    tx_hash AS hash,
    output_id,
    _bgn,
    _tau,
    _tot::number / 1e18 AS total_mkr
FROM __dbt__cte__fact_dssvesttransferrable_create
),  __dbt__cte__fact_dssvesttransferrable_yank as (


with raw as (
SELECT
    trace_index,
    trace_address,
    block_timestamp,
    tx_hash,
    SUBSTR(input, 11) as raw_input_data
FROM ethereum_flipside.core.fact_traces
where to_address = lower('0x6D635c8d08a1eA2F1687a5E46b666949c977B7dd')
and left(input, 10) in ('0x509aaa1d', '0x26e027f1')
)
SELECT 
    block_timestamp,
    tx_hash,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 1, 64)) as _id,
    PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(raw_input_data, 65, 64)) as _end
FROM raw
),  __dbt__cte__fact_mkr_yanks as (


WITH yanks_raw AS (
    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        _end,
        _id
    FROM __dbt__cte__fact_dssvesttransferrable_yank
),

yanks_with_context AS (
    SELECT 
        y.*,
        c._bgn,
        c._tau,
        c.total_mkr,
        CASE 
            WHEN DATEADD(second, y._end, '1970-01-01'::timestamp) > y.ts 
            THEN DATEADD(second, y._end, '1970-01-01'::timestamp)
            ELSE y.ts 
        END AS end_time
    FROM yanks_raw y
    LEFT JOIN __dbt__cte__fact_mkr_vest_creates c
        ON y._id = c.output_id
)

SELECT
    ts,
    hash,
    _id,
    TO_TIMESTAMP(CAST(_bgn AS VARCHAR)) AS begin_time,
    end_time,
    _tau,
    total_mkr AS original_total_mkr,
    (1 - (DATEDIFF(second, '1970-01-01'::timestamp, end_time) - _bgn) / _tau) * total_mkr AS yanked_mkr
FROM yanks_with_context
),  __dbt__cte__fact_mkr_vest_creates_yanks as (


-- MKR Vest Creates
SELECT
    ts,
    hash,
    32110 AS code, -- MKR expense realized
    -total_mkr AS value
FROM __dbt__cte__fact_mkr_vest_creates

UNION ALL

SELECT
    ts,
    hash,
    33110 AS code, -- MKR in vest contracts increases
    total_mkr AS value
FROM __dbt__cte__fact_mkr_vest_creates

UNION ALL

-- MKR Yanks
SELECT
    ts,
    hash,
    32110 AS code, -- MKR expense reversed (yanked)
    yanked_mkr AS value
FROM __dbt__cte__fact_mkr_yanks

UNION ALL

SELECT
    ts,
    hash,
    33110 AS code, -- MKR in vest contracts yanked (decreases)
    -yanked_mkr AS value
FROM __dbt__cte__fact_mkr_yanks
),  __dbt__cte__fact_pause_proxy_mkr_trxns_raw as (


SELECT
    block_timestamp AS ts,
    tx_hash AS hash,
    CAST(raw_amount_precise AS DOUBLE) AS expense,
    to_address AS address
FROM ethereum_flipside.core.ez_token_transfers
WHERE contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' -- MKR token address
  AND from_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb' -- pause proxy
  AND to_address != '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb' -- excluding transfers to itself

UNION ALL

SELECT 
    block_timestamp AS ts,
    tx_hash AS hash,
    -CAST(raw_amount_precise AS DOUBLE) AS expense,
    from_address AS address
FROM ethereum_flipside.core.ez_token_transfers
WHERE contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' -- MKR token address
  AND to_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb' -- pause proxy
    AND from_address NOT IN ('0x8ee7d9235e01e6b42345120b5d270bdb763624c7', '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb') -- excluding initial transfers in and transfers from itself
),  __dbt__cte__fact_dssvesttransferrable_vest as (



SELECT
    block_timestamp,
    tx_hash,
    PC_DBT_DB.PROD.HEX_TO_INT(topics[1]) as _id,
    PC_DBT_DB.PROD.HEX_TO_INT(data) as _max_amt
FROM ethereum_flipside.core.fact_event_logs
where contract_address = lower('0x6D635c8d08a1eA2F1687a5E46b666949c977B7dd')
and topics[0] = '0xa2906882572b0e9dfe893158bb064bc308eb1bd87d1da481850f9d17fc293847'
),  __dbt__cte__fact_mkr_vest_tx as (


SELECT 
    tx_hash AS hash,
    1 AS vested
FROM __dbt__cte__fact_dssvesttransferrable_vest
),  __dbt__cte__fact_pause_proxy_mkr_trxns as (


WITH pause_proxy_mkr_trxns_preunion AS (
    SELECT
        raw.ts,
        raw.hash,
        CASE 
            WHEN vest.vested IS NOT NULL THEN 32120 -- reserved surplus depletion for vested transactions
            ELSE 32110 -- direct protocol surplus impact for non-vested transactions
        END AS code,
        -raw.expense / 1e18 AS value
    FROM __dbt__cte__fact_pause_proxy_mkr_trxns_raw raw
    LEFT JOIN __dbt__cte__fact_mkr_vest_tx vest
        ON raw.hash = vest.hash
)

SELECT
    ts,
    hash,
    code,
    value
FROM pause_proxy_mkr_trxns_preunion

UNION ALL

SELECT
    ts,
    hash,
    32210 AS code, -- MKR contra equity
    -value
FROM pause_proxy_mkr_trxns_preunion
),  __dbt__cte__fact_m2m_levels as (


WITH treasury_tokens AS (
    SELECT token, price_address
    FROM __dbt__cte__dim_treasury_erc20s
    
    UNION ALL
    
    SELECT 'DAI' AS token, '0x6b175474e89094c44da98b954eedeac495271d0f' AS price_address
)

SELECT 
    p.hour AS ts,
    tt.token,
    CASE WHEN tt.token = 'DAI' THEN 1 ELSE p.price END AS price
FROM ethereum_flipside.price.ez_prices_hourly p
INNER JOIN treasury_tokens tt ON p.token_address = tt.price_address
WHERE p.hour >= '2019-11-01'
  AND EXTRACT(HOUR FROM p.hour) = 23
),  __dbt__cte__fact_token_prices as (


WITH tokens AS (
    SELECT token, price_address
    FROM __dbt__cte__dim_treasury_erc20s

    UNION ALL

    SELECT 'MKR' AS token, '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' AS price_address

    UNION ALL

    SELECT 'ETH' AS token, '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS price_address
)

SELECT
    p.hour AS ts,
    t.token,
    p.price
FROM ethereum_flipside.price.ez_prices_hourly p
INNER JOIN tokens t ON lower(p.token_address) = lower(t.price_address)
WHERE p.hour >= '2019-11-01'

UNION ALL

SELECT
    TIMESTAMP '2021-11-09 00:02' AS ts,
    'ENS' AS token,
    44.3 AS price
),  __dbt__cte__fact_eth_prices as (


SELECT *
FROM __dbt__cte__fact_token_prices
WHERE token = 'ETH'
),  __dbt__cte__fact_with_prices as (


WITH unioned_data AS (
    SELECT ts, hash, code, value, 'DAI' AS token, 'Returned Workforce Expenses' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_team_dai_burns
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Liquidation Revenues/Expenses' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_liquidation
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Trading Revenues' AS descriptor, ilk FROM __dbt__cte__fact_trading_revenues
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'MKR Mints' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_mkr_mints
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'MKR Burns' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_mkr_burns
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Interest Accruals' AS descriptor, ilk FROM __dbt__cte__fact_interest_accruals
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'OpEx' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_opex
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'DSR Expenses' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_dsr_expenses
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Other Sin Outflows' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_other_sin_outflows
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Sin Inflows' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_sin_inflows
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'DSR Flows' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_dsr_flows
    UNION ALL
    SELECT ts, hash, code, value, token, 'Treasury Flows' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_treasury_flows
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'Loan Draws/Repays' AS descriptor, ilk FROM __dbt__cte__fact_loan_actions
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'D3M Revenues' AS descriptor, ilk FROM __dbt__cte__fact_d3m_revenues
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'PSM Yield' AS descriptor, ilk FROM __dbt__cte__fact_psm_yield
    UNION ALL
    SELECT ts, hash, code, value, 'DAI' AS token, 'RWA Yield' AS descriptor, ilk FROM __dbt__cte__fact_rwa_yield
    UNION ALL
    SELECT ts, hash, code, value, 'MKR' AS token, 'MKR Vest Creates/Yanks' AS descriptor, NULL AS ilk FROM __dbt__cte__fact_mkr_vest_creates_yanks
    UNION ALL
    SELECT ts, hash, code, value, 'MKR' AS token, 'MKR Pause Proxy Trxns' AS descriptor, NULL AS ilk FROM __dbt__cte__fact_pause_proxy_mkr_trxns
    UNION ALL
    SELECT ts, NULL AS hash, 19999 AS code, 0 AS value, token, 'Currency Translation to Presentation Token' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_m2m_levels
    UNION ALL
    SELECT ts, NULL AS hash, 29999 AS code, 0 AS value, token, 'Currency Translation to Presentation Token' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_m2m_levels
    UNION ALL
    SELECT ts, NULL AS hash, 39999 AS code, 0 AS value, token, 'Currency Translation to Presentation Token' AS descriptor, CAST(NULL AS VARCHAR) AS ilk FROM __dbt__cte__fact_m2m_levels
)

SELECT 
    coa.code,
    u.ts,
    u.hash,
    u.value,
    u.token,
    u.descriptor,
    u.ilk,
    u.value * CASE WHEN u.token = 'DAI' THEN 1 ELSE tp.price END AS dai_value,
    u.value * CASE WHEN u.token = 'DAI' THEN 1 ELSE tp.price END / ep.price AS eth_value,
    ep.price AS eth_price
FROM __dbt__cte__dim_chart_of_accounts coa
LEFT JOIN unioned_data u USING (code)
LEFT JOIN __dbt__cte__fact_token_prices tp 
    ON DATE_TRUNC('day', u.ts) = DATE_TRUNC('day', tp.ts)
    AND EXTRACT(HOUR FROM u.ts) = EXTRACT(HOUR FROM tp.ts)
    AND u.token = tp.token
LEFT JOIN __dbt__cte__fact_eth_prices ep
    ON DATE_TRUNC('day', u.ts) = DATE_TRUNC('day', ep.ts)
    AND EXTRACT(HOUR FROM u.ts) = EXTRACT(HOUR FROM ep.ts)
WHERE u.value IS NOT NULL
),  __dbt__cte__fact_cumulative_sums as (


SELECT
    wp.*,
    SUM(wp.value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS cumulative_ale_token_value,
    SUM(wp.dai_value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS cumulative_ale_dai_value,
    SUM(wp.eth_value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS cumulative_ale_eth_value,
    m2m.price * SUM(wp.value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS dai_value_if_converted_all_once,
    m2m.price/wp.eth_price * SUM(wp.value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS eth_value_if_converted_all_once,
    m2m.price * SUM(wp.value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) - SUM(wp.dai_value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS dai_m2m,
    m2m.price/wp.eth_price * SUM(wp.value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) - SUM(wp.eth_value) OVER (PARTITION BY SUBSTRING(CAST(wp.code AS VARCHAR), 1, 1), wp.token ORDER BY wp.ts) AS eth_m2m
FROM __dbt__cte__fact_with_prices wp
LEFT JOIN __dbt__cte__fact_m2m_levels m2m
    ON wp.token = m2m.token
    AND DATE_TRUNC('day', wp.ts) = DATE_TRUNC('day', m2m.ts)
    -- AND EXTRACT(HOUR FROM wp.ts) = EXTRACT(HOUR FROM m2m.ts)
),  __dbt__cte__fact_incremental_m2m as (


SELECT
    *,
    dai_m2m - COALESCE(LAG(dai_m2m) OVER (PARTITION BY SUBSTRING(CAST(code AS VARCHAR), 1, 1), token ORDER BY ts), 0) AS incremental_dai_m2m,
    eth_m2m - COALESCE(LAG(eth_m2m) OVER (PARTITION BY SUBSTRING(CAST(code AS VARCHAR), 1, 1), token ORDER BY ts), 0) AS incremental_eth_m2m
FROM __dbt__cte__fact_cumulative_sums
WHERE cumulative_ale_token_value > 0
    AND SUBSTRING(CAST(code AS VARCHAR), -4) = '9999'
),  __dbt__cte__fact_final as (


-- Non-M2M entries
SELECT
    code,
    ts,
    hash,
    value,
    token,
    descriptor,
    ilk,
    CASE WHEN descriptor = 'MKR Vest Creates/Yanks' THEN 0 ELSE dai_value END AS dai_value,
    CASE WHEN descriptor = 'MKR Vest Creates/Yanks' THEN 0 ELSE eth_value END AS eth_value,
    DATE(ts) AS dt
FROM __dbt__cte__fact_with_prices
WHERE SUBSTRING(CAST(code AS VARCHAR), -4) <> '9999'

UNION ALL

-- M2M entries
SELECT
    code,
    ts,
    hash,
    NULL AS value,
    token,
    descriptor,
    ilk,
    incremental_dai_m2m AS dai_value,
    incremental_eth_m2m AS eth_value,
    DATE(ts) AS dt
FROM __dbt__cte__fact_incremental_m2m

-- Final filter
WHERE (COALESCE(value, 0) <> 0 OR dai_value <> 0 OR eth_value <> 0)
),  __dbt__cte__fact_accounting_agg as (


with
    chart_of_accounts as (
        select cast(code as varchar) as account_id from __dbt__cte__dim_chart_of_accounts -- CoA simplified
    ),
    periods as (
        select
            distinct(date(hour)) as date
        from ethereum_flipside.price.ez_prices_hourly
        where symbol = 'MKR'
    ),
    accounting as (
        select
            date_trunc('day', acc.ts) as period,
            cast(acc.code as varchar) as account_id,
            acc.dai_value as usd_value,
            acc.value as token_value,
            token
        from __dbt__cte__fact_final acc
    ),
    accounting_agg as (
        select
            date_trunc('day', period) as period,
            account_id,
            token,
            sum(coalesce(token_value,0)) as token_sum_value,
            sum(coalesce(usd_value, 0)) AS usd_sum_value
        from accounting
        group by 1,2,3
    ),
    accounting_liq as (
        select distinct
            period,
            token,
            sum(coalesce(token_sum_value, 0)) over (partition by date_trunc('day', period)) as token_liq_cum,
            sum(coalesce(usd_sum_value, 0)) over (partition by date_trunc('day', period)) as usd_liq_cum
        from accounting_agg
        where account_id in (
            '31210', -- Liquidation Revenues
            '31620'  -- Liquidation Expenses
        )
    )
select
    a.period,
    a.account_id,
    a.token,
    case
        when account_id = '31210' then iff(usd_liq_cum > 0, usd_liq_cum, 0)
        when account_id = '31620' then iff(usd_liq_cum > 0, 0, usd_liq_cum)
        else coalesce(usd_sum_value, 0)
    end as sum_value,
    case
        when account_id = '31210' then iff(token_liq_cum > 0, token_liq_cum, 0)
        when account_id = '31620' then iff(token_liq_cum > 0, 0, token_liq_cum)
        else coalesce(token_sum_value,0)
    end as sum_value_token
from accounting_agg a
left join accounting_liq l
    on a.period = l.period
    and a.token = l.token
),  __dbt__cte__fact_maker_fees_revenue_expenses as (



select
    date(period) as date,
    sum(case
            when
                account_id like '311%' AND account_id NOT IN ('31172', '31173', '31180')-- Stability fees
            then sum_value
        end
    ) as stability_fees,
    sum(case
            when account_id like '313%' -- Trading fees
            then sum_value
        end
    ) as trading_fees,
    sum(case
            when
                account_id like '311%' AND account_id NOT IN ('31172', '31173', '31180') -- Stability fees
                or account_id like '313%' -- Trading fees
            then sum_value
        end
    ) as fees,
    sum(
        case
            when account_id like '311%'  -- Gross Interest Revenues
            then sum_value
        end
    ) as primary_revenue,
    sum(
        case
            when account_id like '312%'  -- Liquidation Revenues
                or account_id like '313%'  -- Trading Revenues
            then sum_value
        end
    ) as other_revenue,
    sum(
        case
            when account_id like '312%'  -- Liquidation Revenues
                or account_id like '313%'  -- Trading Revenues
                or account_id like '311%'  -- Gross Interest Revenues
            then sum_value
        end
    ) as protocol_revenue,
    sum(
        case
            when account_id = '31410' -- MKR Mints
        then sum_value
        end
    ) as token_incentives,
    sum(
        case
            when account_id like '316%'  -- Direct Expenses
            then -sum_value
        end
    ) as direct_expenses,
    sum(
        case
            when account_id like '317%'  -- Indirect Expenses
                or account_id like '321%'  -- MKR Token Expenses
                or account_id like '331%'  -- DS Pause Proxy
            then -sum_value
        end
    ) as operating_expenses,
    sum(
        case
            when account_id like '316%'  -- Direct Expenses
                or account_id like '317%'  -- Indirect Expenses
                or account_id like '321%'  -- MKR Token Expenses
                or account_id like '331%'  -- DS Pause Proxy
                or account_id = '31410'
            then -sum_value
        end
    ) as total_expenses
from  __dbt__cte__fact_accounting_agg
group by 1
),  __dbt__cte__fact_system_surplus_dai as (


with bs_equity as (
    select
        date(ts) as date,
        sum(case when acc.code not like '33%' and acc.code not like '39%' then acc.value else 0 end) as surplus
    from __dbt__cte__fact_final acc
    where code like '3%'
    and acc.code not like '33%' and acc.code not like '39%'
    group by 1
)
select
    date,
    sum(surplus) over (order by date) as surplus,
    'DAI' as token
from bs_equity
),  __dbt__cte__fact_treasury_mkr as (


with prices as (
    SELECT
        date(hour) as date,
        AVG(price) as price
    FROM ethereum_flipside.price.ez_prices_hourly
    WHERE symbol = 'MKR'
    GROUP BY 1
),
mkr_balance_cte as (
    SELECT
        date(block_timestamp) as date,
        MAX_BY(balance,date(block_timestamp))/1e18 as mkr_balance,
        user_address
    FROM ethereum_flipside.core.fact_token_balances
    where user_address in (lower('0xBE8E3e3618f7474F8cB1d074A26afFef007E98FB'), lower('0x8EE7D9235e01e6B42345120b5d270bdB763624C7'))
    and contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2'
    GROUP BY 1, 3
),
date_sequence AS (
    SELECT DISTINCT date
    FROM prices
),
user_addresses AS (
    SELECT DISTINCT user_address
    FROM mkr_balance_cte
),
all_dates_users AS (
    SELECT
        d.date,
        u.user_address
    FROM date_sequence d
    CROSS JOIN user_addresses u
),
joined_balances AS (
    SELECT
        a.date,
        a.user_address,
        p.price,
        m.mkr_balance AS balance_token
    FROM all_dates_users a
    LEFT JOIN prices p ON p.date = a.date
    LEFT JOIN mkr_balance_cte m ON m.date = a.date AND m.user_address = a.user_address
)
,filled_balances AS (
    SELECT
        date,
        user_address,
        price,
        COALESCE(
            balance_token,
            LAST_VALUE(balance_token IGNORE NULLS) OVER (
                PARTITION BY user_address
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance_token,
        COALESCE(
            user_address,
            LAST_VALUE(user_address IGNORE NULLS) OVER (
                PARTITION BY user_address
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS filled_user_address
    FROM joined_balances
)

SELECT
    date,
    SUM(balance_token) amount_mkr,
    SUM(balance_token *price) as amount_usd,
    'MKR' as token
FROM filled_balances
GROUP BY date
),  __dbt__cte__fact_uni_lp_supply as (


WITH token_transfers AS (
    SELECT
        date(block_timestamp) AS date,
        from_address,
        to_address,
        raw_amount_precise::number / 1e18 AS amount
    FROM ethereum_flipside.core.ez_token_transfers
    WHERE contract_address = LOWER('0x517F9dD285e75b599234F7221227339478d0FcC8')
),
daily_mints AS (
    SELECT
        date,
        SUM(amount) AS daily_minted
    FROM token_transfers
    WHERE from_address = LOWER('0x0000000000000000000000000000000000000000')
    GROUP BY date
),
daily_burns AS (
    SELECT
        date,
        SUM(amount) AS daily_burned
    FROM token_transfers
    WHERE to_address = LOWER('0x0000000000000000000000000000000000000000')
    GROUP BY date
),
daily_net_supply AS (
    SELECT
        d.date_day as date,
        COALESCE(m.daily_minted, 0) AS daily_minted,
        COALESCE(b.daily_burned, 0) AS daily_burned,
        COALESCE(m.daily_minted, 0) - COALESCE(b.daily_burned, 0) AS daily_net
    FROM ethereum_flipside.core.dim_dates d
    LEFT JOIN daily_mints m ON d.date_day = m.date
    LEFT JOIN daily_burns b ON d.date_day = b.date
    WHERE date_day < to_date(sysdate())
),
cumulative_supply AS (
    SELECT
        date,
        daily_minted,
        daily_burned,
        daily_net,
        SUM(daily_net) OVER (
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS circulating_supply
    FROM daily_net_supply
)
SELECT 
    date,
    circulating_supply
FROM cumulative_supply
ORDER BY date
),  __dbt__cte__fact_uni_lp_value as (


with token_balances as (
    SELECT
        DATE(block_timestamp) as date,
        case
            when contract_address = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F') then 'DAI'
            when contract_address = lower('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2') then 'MKR'
        END AS token,
        MAX_BY(balance, block_timestamp)::number / 1e18 as balance
    FROM
        ethereum_flipside.core.fact_token_balances
    WHERE
        user_address = lower('0x517F9dD285e75b599234F7221227339478d0FcC8')
        AND contract_address in (
            lower('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2'),
            lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        )
    GROUP BY
        1,
        2
),
prices as (
    SELECT
        date(hour) as date,
        MAX_BY(price, hour) as price,
        symbol
    FROM
        ethereum_flipside.price.ez_prices_hourly
    WHERE
        token_address in (
            lower('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2'),
            lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        )
    GROUP BY
        1,
        3
),
daily_prices_by_token as (
    SELECT
        b.date,
        p.price * b.balance as balance_usd,
        b.token
    FROM
        token_balances b
        LEFT JOIN prices p on p.date = b.date
        AND p.symbol = b.token
)
SELECT
    date,
    sum(balance_usd) as amount_usd
FROM 
    daily_prices_by_token
GROUP BY 1
ORDER BY 1 DESC
),  __dbt__cte__fact_treasury_lp_balances as (



with
dates as (
    SELECT
        DISTINCT date(hour) as date
    FROM ethereum_flipside.price.ez_prices_hourly
    WHERE symbol = 'MKR'
)
, treasury_balance as (
    select
        date(block_timestamp) as date,
        MAX(balance)::number / 1e18 as treasury_lp_balance
    from
        ethereum_flipside.core.fact_token_balances
    where
        contract_address = LOWER('0x517F9dD285e75b599234F7221227339478d0FcC8')
        and user_address = lower('0xBE8E3e3618f7474F8cB1d074A26afFef007E98FB')
    GROUP BY
        1
),
value_per_token_cte as (
    SELECT
        s.date,
        v.amount_usd / s.circulating_supply as value_per_token_usd,
        s.circulating_supply
    FROM
        __dbt__cte__fact_uni_lp_supply s
        LEFT JOIN __dbt__cte__fact_uni_lp_value v ON v.date = s.date
    where
        value_per_token_usd is not null
)
, filled_data as (
    SELECT
        d.date,
        LAST_VALUE(t.treasury_lp_balance IGNORE NULLS) OVER (
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as amount_native,
        LAST_VALUE(v.value_per_token_usd IGNORE NULLS) OVER (
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as value_per_token_usd
    FROM
        dates d
        LEFT JOIN treasury_balance t ON d.date = t.date
        LEFT JOIN value_per_token_cte v ON d.date = v.date
)
SELECT
    date,
    amount_native,
    value_per_token_usd,
    amount_native * value_per_token_usd as amount_usd,
    'UNI V2: DAI-MKR' as token
FROM
    filled_data
WHERE amount_native is not null
ORDER BY
    date
),  __dbt__cte__fact_treasury_usd as (


with agg as(
    SELECT
        date,
        surplus as amount_usd
    FROM
        __dbt__cte__fact_system_surplus_dai
    UNION ALL
    SELECT
        date,
        amount_usd
    FROM
        __dbt__cte__fact_treasury_mkr
    UNION ALL
    SELECT
        date,
        amount_usd
    FROM
        __dbt__cte__fact_treasury_lp_balances
)
SELECT
    date,
    SUM(amount_usd) AS treasury_usd
FROM
    agg
GROUP BY
    1
ORDER BY
    1 DESC
),  __dbt__cte__fact_net_treasury_usd as (


with agg as(
    SELECT
        date,
        surplus as amount_usd
    FROM
        __dbt__cte__fact_system_surplus_dai
    UNION ALL
    SELECT
        date,
        amount_usd
    FROM
        __dbt__cte__fact_treasury_lp_balances
)
SELECT
    date,
    SUM(amount_usd) AS net_treasury_usd
FROM
    agg
GROUP BY
    1
ORDER BY
    1 DESC
),  __dbt__cte__dim_gem_join_addresses as (


with join_addresses as (
    select
        '0x' || SUBSTR(topics [1], 27) as join_address,
        *
    from
        ethereum_flipside.core.fact_event_logs
    where
        topics [0] = lower(
            '0x65fae35e00000000000000000000000000000000000000000000000000000000'
        )
        and contract_address = lower('0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b')
)
, contract_creation_hashes as(
    select
        address as join_address,
        created_tx_hash
    from
        ethereum_flipside.core.dim_contracts
    where
        address in (SELECT lower(join_address) FROM join_addresses)
)
SELECT
    '0x' || RIGHT(t.input, 40) as gem_address,
    h.join_address
FROM
     contract_creation_hashes h
LEFT JOIN ethereum_flipside.core.fact_traces t ON h.created_tx_hash = t.tx_hash
),  __dbt__cte__fact_maker_tvl_by_asset as (



WITH weth AS (
    SELECT DISTINCT
        m.symbol,
        g.gem_address,
        g.join_address,
        m.decimals
    FROM
        __dbt__cte__dim_gem_join_addresses g
        LEFT JOIN ethereum_flipside.price.ez_asset_metadata m ON g.gem_address = m.token_address
    WHERE
        symbol IS NOT NULL
),
daily_balances AS (
    SELECT
        DATE(t.block_timestamp) AS date,
        t.user_address,
        t.contract_address,
        w.symbol,
        w.decimals,
        AVG(t.balance / POWER(10, w.decimals)) AS amount_native
    FROM
        ethereum_flipside.core.fact_token_balances t
        JOIN weth w ON t.user_address = w.join_address AND t.contract_address = w.gem_address
    GROUP BY 1, 2, 3, 4, 5
),
date_series AS (
    SELECT date_day as date 
    FROM ethereum_flipside.core.dim_dates
    WHERE date_day < to_date(sysdate())
),
all_combinations AS (
    SELECT DISTINCT
        d.date,
        db.user_address,
        db.contract_address
    FROM
        date_series d
        CROSS JOIN (SELECT DISTINCT user_address, contract_address FROM daily_balances) db
),
forward_filled_balances AS (
    SELECT
        ac.date,
        ac.user_address,
        ac.contract_address,
        LAST_VALUE(db.symbol IGNORE NULLS) OVER (
            PARTITION BY ac.user_address, ac.contract_address
            ORDER BY ac.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS symbol,
        LAST_VALUE(db.decimals IGNORE NULLS) OVER (
            PARTITION BY ac.user_address, ac.contract_address
            ORDER BY ac.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS decimals,
        LAST_VALUE(db.amount_native IGNORE NULLS) OVER (
            PARTITION BY ac.user_address, ac.contract_address
            ORDER BY ac.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS amount_native
    FROM
        all_combinations ac
        LEFT JOIN daily_balances db 
            ON ac.date = db.date 
            AND ac.user_address = db.user_address 
            AND ac.contract_address = db.contract_address 
),
usd_values AS (
    SELECT
        ffb.date,
        ffb.user_address,
        ffb.contract_address,
        ffb.symbol,
        ffb.amount_native,
        ffb.amount_native * COALESCE(
            p.price,
            FIRST_VALUE(p.price) OVER (
                PARTITION BY ffb.contract_address
                ORDER BY CASE WHEN p.price IS NOT NULL THEN ffb.date END DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS amount_usd
    FROM
        forward_filled_balances ffb
        LEFT JOIN ethereum_flipside.price.ez_prices_hourly p 
            ON p.hour = DATE_TRUNC('day', ffb.date) 
            AND p.token_address = ffb.contract_address
    WHERE
        ffb.amount_native IS NOT NULL
)
SELECT
    date,
    SUM(amount_native) AS total_amount_native,
    symbol,
    SUM(amount_usd) AS total_amount_usd
FROM
    usd_values
GROUP BY 1, 3
ORDER BY 1
),  __dbt__cte__fact_maker_tvl as (


SELECT
    date,
    sum(total_amount_usd) as tvl_usd
FROM
    __dbt__cte__fact_maker_tvl_by_asset
GROUP BY
    1
),  __dbt__cte__fact_dai_eth_supply as (


with eth_raw as(
    select
        block_timestamp,
        CASE
            WHEN lower(FROM_ADDRESS) = lower('0x0000000000000000000000000000000000000000') THEN AMOUNT
            WHEN lower(TO_ADDRESS) = lower('0x0000000000000000000000000000000000000000') THEN - AMOUNT
        END AS amount
    from
        ethereum_flipside.core.ez_token_transfers
    where
        lower(contract_address) = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        and (
            lower(FROM_ADDRESS) = lower('0x0000000000000000000000000000000000000000')
            or lower(TO_ADDRESS) = lower('0x0000000000000000000000000000000000000000')
        )
),
daily_amounts AS (
    SELECT
        date(block_timestamp) as date,
        SUM(amount) as daily_amount
    FROM eth_raw
    GROUP BY date(block_timestamp)
)
SELECT
    date,
    SUM(daily_amount) OVER (ORDER BY date) as dai_supply,
    'Ethereum' as chain
FROM daily_amounts
ORDER BY date DESC
),  __dbt__cte__fact_dai_dsr_supply as (



WITH
  deltas AS (
    SELECT
      '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' AS wallet,
      DATE(block_timestamp) AS dt,
      CAST(rad AS DOUBLE) AS delta,
    FROM
      ethereum_flipside.maker.fact_VAT_move
    WHERE dst_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    UNION ALL
    SELECT
      '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' AS wallet,
      DATE(block_timestamp) AS dt,
      - CAST(rad AS DOUBLE) AS delta
    FROM
      ethereum_flipside.maker.fact_VAT_move
    WHERE src_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
    UNION ALL
    SELECT
      '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7' AS wallet,
        DATE(block_timestamp) as dt,
      CAST(rad AS DOUBLE) AS delta
    FROM
      ethereum_flipside.maker.fact_VAT_suck
    WHERE v_address = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
)
, daily_supply as (
    SELECT
        dt,
        sum(delta) as dai_supply
    FROM deltas
    GROUP BY 1
)
SELECT
    dt as date,
    SUM(dai_supply) OVER (ORDER BY dt) as dai_supply,
    'Ethereum' as chain
FROM daily_supply
),  __dbt__cte__fact_dai_supply_by_chain as (



select * from __dbt__cte__fact_dai_eth_supply
union all
select * from __dbt__cte__fact_dai_dsr_supply
),  __dbt__cte__fact_dai_supply as (




SELECT date, SUM(dai_supply) as outstanding_supply FROM __dbt__cte__fact_dai_supply_by_chain
GROUP BY 1
),  __dbt__cte__fact_maker_fdv_and_turnover as (



SELECT
    date,
    shifted_token_h24_volume_usd as token_volume,
    shifted_token_price_usd * 1005577 as fully_diluted_market_cap,
    shifted_token_h24_volume_usd / shifted_token_market_cap as token_turnover_circulating,
    shifted_token_h24_volume_usd / fully_diluted_market_cap as token_turnover_fdv
FROM pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
where coingecko_id = 'maker'
and shifted_token_market_cap > 0
and fully_diluted_market_cap > 0
order by date desc
),  __dbt__cte__fact_mkr_tokenholder_count as (


WITH filtered_balances AS (
    SELECT
        DATE(block_timestamp) AS date,
        address,
        MAX_BY(balance_token / 1e18, block_timestamp) AS balance_token
    FROM pc_dbt_db.prod.fact_ethereum_address_balances_by_token
    WHERE contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' -- set token contract address
    GROUP BY 1, 2
),
unique_dates AS (
    SELECT DISTINCT DATE(block_timestamp) AS date
    FROM pc_dbt_db.prod.fact_ethereum_address_balances_by_token
    where block_timestamp > '2017-12-19' -- set token contract creation date
),
addresses AS (
    SELECT
        address,
        MIN(DATE(block_timestamp)) AS first_date
    FROM pc_dbt_db.prod.fact_ethereum_address_balances_by_token
    WHERE contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' -- set token contract address
    GROUP BY address
),
all_combinations AS (
    SELECT
        ud.date,
        a.address
    FROM unique_dates ud
    JOIN addresses a
    ON ud.date >= a.first_date
)
, joined_balances AS (
    SELECT
        ac.date,
        ac.address,
        fb.balance_token
    FROM all_combinations ac
    LEFT JOIN filtered_balances fb
        ON ac.date = fb.date
        AND ac.address = fb.address
)
, filled_balances AS (
    SELECT
        date,
        address,
        COALESCE(
            balance_token,
            LAST_VALUE(balance_token IGNORE NULLS) OVER (
                PARTITION BY address ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance_token
    FROM joined_balances
)

select date, count(*) as tokenholder_count from filled_balances
where balance_token > 0
group by date
order by date desc
), fees_revenue_expenses AS (
        SELECT
            date,
            stability_fees,
            trading_fees,
            fees,
            primary_revenue,
            other_revenue,
            protocol_revenue,
            token_incentives,
            direct_expenses,
            operating_expenses,
            total_expenses
        FROM __dbt__cte__fact_maker_fees_revenue_expenses
    )
    , treasury_usd AS (
        SELECT date, treasury_usd FROM __dbt__cte__fact_treasury_usd
    )
    , treasury_native AS (
        SELECT date, amount_mkr as treasury_native FROM __dbt__cte__fact_treasury_mkr
    )
    , net_treasury AS (
        SELECT date, net_treasury_usd FROM __dbt__cte__fact_net_treasury_usd
    )
    , tvl_metrics AS (
        SELECT date, tvl_usd as net_deposit, tvl_usd as tvl FROM __dbt__cte__fact_maker_tvl
    )
    , outstanding_supply AS (
        SELECT date, outstanding_supply FROM __dbt__cte__fact_dai_supply
    )
    , token_turnover_metrics as (
        select
            date
            , token_turnover_circulating
            , token_turnover_fdv
            , token_volume
        from __dbt__cte__fact_maker_fdv_and_turnover
    )
    , price_data as (
    select
        date as date,
        shifted_token_price_usd as price,
        shifted_token_market_cap as market_cap,
        t2.total_supply * price as fdmc
    from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted_gold t1
    inner join
        (
            select
                token_id, coalesce(token_max_supply, token_total_supply) as total_supply
            from PC_DBT_DB.PROD.fact_coingecko_token_realtime_data
            where token_id = 'maker'
        ) t2
        on t1.coingecko_id = t2.token_id
    where
        coingecko_id = 'maker'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select
        dateadd('day', -1, to_date(sysdate())) as date,
        token_current_price as price,
        token_market_cap as market_cap,
        coalesce(token_max_supply, token_total_supply) * price as fdmc
    from PC_DBT_DB.PROD.fact_coingecko_token_realtime_data
    where token_id = 'maker'
)
    , token_holder_data as (
        select
            date
            , tokenholder_count
        from __dbt__cte__fact_mkr_tokenholder_count
    )


select
    date
    , COALESCE(stability_fees,0) as stability_fees
    , COALESCE(trading_fees, 0) AS trading_fees
    , COALESCE(fees, 0) AS fees
    , COALESCE(primary_revenue, 0) AS primary_revenue
    , COALESCE(other_revenue, 0) AS other_revenue
    , COALESCE(protocol_revenue, 0) AS protocol_revenue
    , COALESCE(token_incentives, 0) AS token_incentives
    , COALESCE(operating_expenses, 0) AS operating_expenses
    , COALESCE(direct_expenses, 0) AS direct_expenses
    , COALESCE(total_expenses, 0) AS total_expenses
    , COALESCE(protocol_revenue - total_expenses, 0) AS earnings
    , COALESCE(treasury_usd, 0) AS treasury_usd
    , COALESCE(treasury_native, 0) AS treasury_native
    , COALESCE(net_treasury_usd, 0) AS net_treasury_usd
    , COALESCE(net_deposit, 0) AS net_deposits
    , COALESCE(outstanding_supply, 0) AS outstanding_supply
    , COALESCE(tvl, 0) AS tvl
    , COALESCE(price, 0) AS price
    , COALESCE(fdmc, 0) AS fdmc
    , COALESCE(market_cap, 0) AS market_cap
    , COALESCE(token_volume, 0) AS token_volume
    , COALESCE(token_turnover_fdv, 0) AS token_turnover_fdv
    , COALESCE(token_turnover_circulating, 0) AS token_turnover_circulating
    , COALESCE(tokenholder_count, 0) AS tokenholder_count
FROM token_holder_data
left join treasury_usd using (date)
left join treasury_native using (date)
left join net_treasury using (date)
left join tvl_metrics using (date)
left join outstanding_supply using (date)
left join token_turnover_metrics using (date)
left join price_data using (date)
left join fees_revenue_expenses using (date)
where date < to_date(sysdate())
