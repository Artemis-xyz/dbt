{{ config(materialized="table") }}

select id
from (
    values
        ('BLP7UHUg1yNry94Qk3sM8pAfEyDhTZirwFghw9DoBjn7'),
        ('ByPbo7yGcsfrEXet3ip3DcMKf4hwhUv71b6aAU9umBdu'),
        ('819tcubKaRghnpDKnbJhUhyuPFoBMKdTREMoAKBKb7xf'),
        ('FqYY63EPjYZXqsWqvGxJ27EKM4wptmmoykkB6km9743U'),
        ('7qwUjxLqLu6sLMYThaF6CC92XkqqdxMHYhZdVtGkefX5')
) as pools(id)
