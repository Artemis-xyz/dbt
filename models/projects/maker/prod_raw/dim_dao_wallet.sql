{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="dim_dao_wallet"
    )
}}

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
