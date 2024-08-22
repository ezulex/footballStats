{{ config(alias='v_stg_players') }}

WITH ranked_players AS (
    SELECT
        teams_team_id,
        player_id,
        player_name,
        player_nickname,
        jersey_number,
        country_id,
        country_name,
        load_date,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY load_date DESC) AS rn
    FROM {{ source('statsbomb', 'lineups_ext') }}
)
SELECT
    teams_team_id,
    player_id,
    player_name,
    player_nickname,
    jersey_number,
    country_id,
    country_name,
    load_date
FROM ranked_players
WHERE rn = 1

