{{ config(materialized='incremental')    }}

{%- set source_model = "matches_stg"   -%}
{%- set src_pk = "MATCH_HK"          -%}
{%- set src_nk = "MATCH_ID"          -%}
{%- set src_ldts = "LOAD_DATETIME"      -%}
{%- set src_source = "SOURCE"    -%}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                   src_source=src_source, source_model=source_model) }}