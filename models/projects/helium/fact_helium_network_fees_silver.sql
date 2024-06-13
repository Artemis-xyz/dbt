{{ config(snowflake_warehouse="HELIUM") }}
with program as
(
   SELECT
      *
    FROM
      solana_flipside.core.fact_events
    WHERE
     program_id = 'credMBJhYFzfn7NxBMdU4aUqFggAjgztaCcv2Fo6fPT'
     AND SUCCEEDED = 'TRUE'
    order by 1 desc
)
SELECT
  date(block_timestamp) as date,
  SUM(GET_PATH(inner_instruction, 'instructions[0]:parsed:info:amount'))::INT/1e5 AS network_fees,
  'solana' AS chain,
  'helium' AS protocol
FROM program p,
   LATERAL flatten(input => p.signers) a
WHERE a.value in ('mobMc6Q18xT78cRiExgVbhadEKHiTUm6ZDnSZn2cU8h', 'iotdbYfWqPiDa5MruziFHxBsjHamKuEnMhkPFoW4mKE')
and date < to_date(sysdate())
group by 1
order by 1 desc
