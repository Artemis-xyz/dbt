{% macro chainlink_non_circulating_supply_addresses() %}
    SELECT LOWER(address) AS address
    FROM (
        VALUES
        ('0x98c63b7b319dfbdf3d811530f2ab9dfe4983af9d'),
        ('0x75398564ce69b7498da10a11ab06fd8ff549001c'),
        ('0x5560d001f977df5e49ead7ab0bdd437c4ee3a99e'),
        ('0xbe6977e08d4479c0a6777539ae0e8fa27be4e9d6'),
        ('0xdad22a85ef8310ef582b70e4051e543f3153e11f'),
        ('0xe0362f7445e3203a496f6f8b3d51cbb413b69be2'),
        ('0x5a8e77bc30948cc9a51ae4e042d96e145648bb4c'),
        ('0xe0b66bfc7344a80152bfec954942e2926a6fca80'),
        ('0xa42d0a18b834f52e41beddeaa2940165db3da9a3'),
        ('0x276f695b3b2c7f24e7cf5b9d24e416a7f357adb7'),
        ('0x5eab1966d5f61e52c22d0279f06f175e36a7181e'),
        ('0x959815462eec5fff387a2e8a6871d94323d371de'),
        ('0xb9b012cad0a7c1b10cbe33a1b3f623b06fad1c7c'),
        ('0xfb682b0de4e0093835ea21cfabb5449ca9ac9e5e'),
        ('0x3264225f2fd3bb8d5dc50587ea7506aa8638b966'),
        ('0x8d34d66bdb2d1d6acd788a2d73d68e62282332e7'),
        ('0x4a87ece3efffcb012fbe491aa028032e07b6f6cf'),
        ('0x57ec4745258e5a4c73d1a82636dc0fe291e3ee9f'),
        ('0x37398a324d35c942574650b9ed2987bc640bad76'),
        ('0xd321948212663366503e8dccde39cc8e71c267c0'),
        ('0x55b0ba1994d68c2ab0c01c3332ec9473de296137'),
        ('0xd48133c96c5fe8d41d0cbd598f65bf4548941e27'),
        ('0x9c17f630dbde24eece8fd248faa2e51f690ff79b'),
        ('0x35a5dc3fd1210fe7173add3c01144cf1693b5e45'),
        ('0x0dffd343c2d3460a7ead2797a687304beb394ce0'),
        ('0x76287e0f7b107d1c9f8f01d5afac314ea8461a04'),
        ('0x9bbb46637a1df7cadec2afca19c2920cddcc8db8'),
        ('0x7594eb0ca0a7f313befd59afe9e95c2201a443e4'),
        ('0x8652fb672253607c0061677bdcafb77a324de081'),
        ('0x157235a3cc6011d9c26a010875c2550246aabcca'),
        ('0xa71bbbd288a4e288cfdc08bb2e70dcd74da4486d'),
        ('0xec640a90e9a30072158115b7c0253f2689ee6547'),
        ('0x2a6ab3b0c96377bd20ae47e50ae426a8546a4ae9')
    ) AS addresses(address)
{% endmacro%}