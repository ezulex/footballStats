{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model:
    statsbomb: 'positions_ext'
derived_columns:
  SOURCE: "CAST('statsbomb' as text)"
  LOAD_DATETIME: "load_date"
  EFFECTIVE_FROM: "load_date"
  START_DATE: "load_date"
  END_DATE: "TO_DATE('9999-12-31', 'YYYY-MM-DD')"
hashed_columns:
  POSITION_HK: "position"
  MATCH_HK: "match_id"
  PLAYER_HK: "teams_lineup_player_id"
  POSITION_HASHDIFF:
    is_hashdiff: true
    columns:
        - "position"
        - "start_reason"
        - "end_reason"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
