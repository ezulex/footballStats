{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: {{ 'v_stg_clubs' }}
derived_columns:
  SOURCE: "CAST('statsbomb' as text)"
  LOAD_DATETIME: "TO_DATE(load_date, 'YYYYMMDD')"
  EFFECTIVE_FROM: "TO_DATE(load_date, 'YYYYMMDD')"
  START_DATE: "TO_DATE(load_date, 'YYYYMMDD')"
  END_DATE: "TO_DATE('9999-12-31', 'YYYY-MM-DD')"
hashed_columns:
  CLUB_HK: "club_id"
  CLUB_HASHDIFF:
    is_hashdiff: true
    columns:
        - "club_name"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
