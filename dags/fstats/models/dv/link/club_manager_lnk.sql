{{ config(materialized='incremental', is_auto_end_dating='true')  }}


{%- set source_model = "managers_stg" -%}
{%- set src_pk = "MANAGER_CLUB_HK" -%}
{%- set src_dfk = "CLUB_HK"       -%}
{%- set src_sfk = "MANAGER_HK"         -%}
{%- set src_start_date = "START_DATE" -%}
{%- set src_end_date = "END_DATE"     -%}

{%- set src_eff = "EFFECTIVE_FROM"    -%}
{%- set src_ldts = "LOAD_DATETIME"    -%}
{%- set src_source = "SOURCE"  -%}

{{ automate_dv.eff_sat(src_pk=src_pk, src_dfk=src_dfk, src_sfk=src_sfk,
                       src_start_date=src_start_date,
                       src_end_date=src_end_date,
                       src_eff=src_eff, src_ldts=src_ldts,
                       src_source=src_source,
                       source_model=source_model) }}