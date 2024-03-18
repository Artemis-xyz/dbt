{{ config(materialized="incremental", unique_key=["tx_hash", "chain"]) }}

{{ mux_decoding("arbitrum", "0x3e0199792Ce69DC29A0a36146bFa68bd7C8D6633") }}
union all
{{ mux_decoding("bsc", "0x855E99F768FaD76DD0d3EB7c446C0b759C96D520") }}
union all
{{ mux_decoding("avalanche", "0x0bA2e492e8427fAd51692EE8958eBf936bEE1d84") }}
union all
{{ mux_decoding("optimism", "0xc6BD76FA1E9e789345e003B361e4A0037DFb7260") }}
