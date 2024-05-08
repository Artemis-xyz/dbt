{{
    config(
        materialized="table",
        snowflake_warehouse="POLYGON_SM",
    )
}}

{{ filter_p2p_token_transfers("polygon", blacklist=(
    "0xc79fc2885e207e1c4cc69cf94402dd1a5642452e",
    "0xaaa5b9e6c589642f98a1cda99b9d024b8407285a",
    "0x46d502fac9aea7c5bc7b13c8ec9d02378c33d36f",
    "0xe5cf781d9e6e92b051ffb8037a5d81981863ea82",
    "0x228b5c21ac00155cf62c57bcc704c0da8187950b",
    "0xfcb54da3f4193435184f3f647467e12b50754575",
    "0x8db0a6d1b06950b4e81c4f67d1289fc7b9359c7f",
    "0xa9536b9c75a9e0fae3b56a96ac8edf76abc91978",
    "0x229b1b6c23ff8953d663c4cbb519717e323a0a84",
    "0xa27b6853d759c03b3ac3714a97322c90b9c79316",
    "0xb541a306dd240ef04fb5e7e0db9a3c6cb7ddbb07",
    "0xefb3009ddac87e8144803d78e235e7fb4cd36e61",
    "0x51de72b17c7bd12e9e6d69eb506a669eb6b5249e",
    "0xef938b6da8576a896f6e0321ef80996f4890f9c4",
    "0x311434160d7537be358930def317afb606c0d737",
    "0x20c750c57c3bc5145af4b7a33d4fb66a8e79fe05"
))}}