with renzodeposits as (
  select
    *,
    TRY_TO_NUMBER(DECODED_LOG:amount :: string) AS amount,
    DECODED_LOG:depositor :: string AS depositor,
    TRY_TO_NUMBER(DECODED_LOG:ezETHMinted :: string) AS ezETHMinted,
    DECODED_LOG:token :: string AS token
  from
    ethereum_flipside.core.ez_decoded_event_logs
  where
    topics [0] :: string in (
      '0x4e2ca0515ed1aef1395f66b5303bb5d6f1bf9d61a353fa53f73f8ac9973fa9f6',
      '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'
    )
    and lower(contract_address) = lower('0x74a09653A083691711cF8215a6ab074BB4e99ef5')
)
select
  date_trunc('day', block_timestamp) as day,
  SUM(amount / 1e18) as deposits,
  COUNT(DISTINCT(depositor)) as unique_depositors
FROM
  renzodeposits
GROUP BY
  1
order by
  day desc