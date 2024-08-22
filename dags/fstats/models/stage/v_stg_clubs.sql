{{ config(alias='v_stg_clubs') }}

SELECT DISTINCT club_id, club_name, load_date
FROM (
    SELECT home_team_home_team_id AS club_id,
           home_team_home_team_name AS club_name,
           load_date
    FROM {{ source('statsbomb', 'matches_ext') }}
    UNION
    SELECT away_team_away_team_id AS club_id,
           away_team_away_team_name AS club_name,
           load_date
    FROM {{ source('statsbomb', 'matches_ext') }}
) AS clubs

