{{ config(materialized="table") }}

select id
from (
    values
        ('8sLbNZoA1cfnvMJLPfp98ZLAnFSYCFApfJKMbiXNLwxj'),
        ('3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv'),
        ('2zVV22uNWdJNmkXpj5vCrMzwHGBoJdsyV7qACh29sK1w'),
        ('AS5MV3ear4NZPMWXbCsEz3AdbCaXEnq4ChdaWsvLgkcM'),
        ('DbsTAmxnFAWRvigSks6DahpKi5Ypz3w2BMKEejTJcGGm'),
        ('BZtgQEyS6eXUXicYPHecYQ7PybqodXQMvkjUbP4R8mUU'),
        ('EEkMbgYJzgYNUJsnURtL3jkDdkAws5RbDL7ukCUFNRMV')
) as pools(id)
