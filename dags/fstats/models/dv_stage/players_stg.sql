{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model:  {{ 'v_stg_players' }}
derived_columns:
  SOURCE: "CAST('statsbomb' as text)"
  LOAD_DATETIME: "load_date"
  EFFECTIVE_FROM: "load_date"
  START_DATE: "load_date"
  END_DATE: "TO_DATE('9999-12-31', 'YYYY-MM-DD')"
hashed_columns:
  PLAYER_HK: "player_id"
  CLUB_HK: "teams_team_id"
  PLAYER_CLUB_HK:
    - "player_id"
    - "teams_team_id"
  PLAYER_HASHDIFF:
    is_hashdiff: true
    columns:
        - "player_name"
        - "player_nickname"
        - "jersey_number"
        - "country_name"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     null_columns=none,
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
