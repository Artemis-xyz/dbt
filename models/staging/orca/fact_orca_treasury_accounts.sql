    {{
        config(
            materialized="table",
            snowflake_warehouse="ORCA",
        )
    }}

SELECT
    *
FROM values
    ('EXfMuea3TGiKJCvbhySsX6UPMue6wA6NTPDPBWBUyujy', NULL, 'orca', 'solana', NULL, NULL, 'treasury', SYSDATE()::TIMESTAMP_NTZ),
    ('3qPbC7P9baPCXxz2Duqk2Qmbj21ap8pRRbRY8sfobKje', NULL, 'orca', 'solana', NULL, NULL, 'treasury', SYSDATE()::TIMESTAMP_NTZ),
    ('9pvzg4ocGHZgGsoviHFjTvAR9oEjRe4bbyjMWXzjn3U1', NULL, 'orca', 'solana', NULL, NULL, 'treasury', SYSDATE()::TIMESTAMP_NTZ),
    ('5Je5sHZL5HrF8YDiwZDbpnsRSzzbeNYAjQSx4e2U5Uxd', NULL, 'orca', 'solana', NULL, NULL, 'treasury', SYSDATE()::TIMESTAMP_NTZ),
    ('GwH3Hiv5mACLX3ufTw1pFsrhSPon5tdw252DBs4Rx4PV', NULL, 'orca', 'solana', NULL, NULL, 'treasury', SYSDATE()::TIMESTAMP_NTZ),
    ('EDqUbyQMxf4npt6zkQdcrc45F523azvTe6VeazJNHWYz', NULL, 'orca', 'solana', NULL, NULL, 'treasury', SYSDATE()::TIMESTAMP_NTZ),
    ('5ooCx5vKiV2ZxAEKNNHAJjAJ7BARfLUZPGvgiApZjgFD', NULL, 'orca', 'solana', NULL, NULL, 'treasury', SYSDATE()::TIMESTAMP_NTZ)
AS orca_treasury_accounts(address, name, artemis_application_id, chain, is_token, is_fungible, type, last_updated)