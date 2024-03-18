create or replace function
    map_token_id_to_address(
        token_id float, coin_0 string, coin_1 string, coin_2 string, coin_3 string
    )
returns string
language javascript
as '
    var tokens = [COIN_0, COIN_1, COIN_2, COIN_3];
    return tokens[parseInt(TOKEN_ID, 10)];
'
;
