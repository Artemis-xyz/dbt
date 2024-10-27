{{ config(
    materialized="table",
    snowflake_warehouse="METAPLEX"
) }}

SELECT 
    program_id::TEXT AS program_id, 
    program_name
FROM (
    VALUES
        ('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s', 'Token Metadata'),
        ('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY', 'Bubblegum'),
        ('CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d', 'Core'),
        ('CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR', 'Candy Machine v3'),
        ('cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ', 'Candy Machine v2'),
        ('cndyAnrLdqq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ', 'Candy Machine v1'),
        ('hausS13jsjafwWwGqZTUQRmWyvyxn9EQpqMwV1PBBmk', 'Auction House'),
        ('neer8g6yJq2mQM6KbnViEDAD4gr3gRZyMMf4F2p3MEh', 'Auctioneer'),
        ('Guard1JwRhJkVH6XZhzoYxeBVQe872VH6QggF4BWmS9g', 'Candy Guard'),
        ('CMAGAKJ67e9hRZgfC5SFTbZH8MgEmtqazKXjmkaJjWTJ', 'Core Candy Guard'),
        ('CMACYFENjoBMHzapRXyo1JZkVS6EtaDDzkjMrmQLvr4J', 'Core Candy Machine'),
        ('gdrpGjVffourzkdDRrQmySw4aTHr8a3xmQzzxSwFD1a', 'Gumdrop'),
        ('hyDQ4Nz1EgyegS6JfenyKwKzYxRsCWCriYSAjtzP4Vg', 'Hydra'),
        ('1NSCRfGeyo7wPUazGbaPBUsTM49e1k2aXewHGARfzSo', 'Inscriptions'),
        ('MPL4o4wMzndgh8T1NVDxELQCj5UQfYTYEkabX3wNKtb', 'MPL-Hybrid'),
        ('auth9SigNpDKz4sJJ1DfCTuZrZNSAgh9sFD3rboVmgg', 'Token Auth Rules')
) AS t(program_id, program_name)
