{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="ETHEREUM",
        database="ethereum",
        schema="raw",
        alias="ez_token_transfers",
    )
}}

{{ 
    get_token_transfer_filtered(
        'ethereum'
        , limit_number=300
        , blacklist=(
            "0x130914e1b240a7f4c5d460b7d3a2fd3846b576fa",
            "0x087b81c5312bcb45179a05aff5aec5cdddc789b6",
            "0x62cc7d1790e5a9470be22ae9f14065cbfe44bf10",
            "0xddb3422497e61e13543bea06989c0789117555c5",
            "0x630d98424efe0ea27fb1b3ab7741907dffeaad78",
            "0x8c3ee4f778e282b59d42d693a97b80b1ed80f4ee",
            "0x3f7aff0ef20aa2e646290dfa4e67611b2220c597",
            "0xc89b4a8a121dd3e726fe7515e703936cf83e3350",
            "0xbd4b2dd8fbcecb2af5904ff5f218037b0f693275",
            "0x4d67edef87a5ff910954899f4e5a0aaf107afd42",
            "0x84fa8f52e437ac04107ec1768764b2b39287cb3e",
            "0xa2253b08b04a61441c0cba8e83226ac4f69405b7",
            "0x33d6064f0dfb62462a74049f30909ddd4f683ba2",
            "0x432d03d11a324b90ba14141d4bc75b68d2350bb8",
            "0x04a5198063e45d84b1999516d3228167146417a6",
            "0x15f20f9dfdf96ccf6ac96653b7c0abfe4a9c9f0f",
            "0xdfef6416ea3e6ce587ed42aa7cb2e586362cbbfa",
            "0xbe77212a6c7f55567470c2c95aff7b0b0e0c3ef5",
            "0xd2b274cfbf9534f56b59ad0fb7e645e0354f4941",
            "0x2b591e99afe9f32eaa6214f7b7629768c40eeb39",
            "0xba12222222228d8ba445958a75a0704d566bf2c8"
        )
        , whitelist=(
            "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409",
            "0x0c10bf8fcb7bf5412187a595ab97a3609160b5c6",
            "0x853d955acef822db058eb8505911ed77f175b99e"
        )
        , include_full_history="TRUE"
    ) 
}}