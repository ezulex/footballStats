{{ config(alias='mart_rude_players', materialized='view') }}


WITH club_player AS (
    SELECT
        pcl.CLUB_HK,
        pcl.PLAYER_HK,
        ps.player_name,
        cs.club_name
    FROM {{ ref('player_club_lnk') }} AS pcl
    INNER JOIN {{ ref('players_sat') }} AS ps ON ps.PLAYER_HK = pcl.PLAYER_HK
    INNER JOIN {{ ref('clubs_sat') }} AS cs ON cs.CLUB_HK = pcl.CLUB_HK
),
player_card AS (
    SELECT
        pc.PLAYER_HK,
        pc.CARD_HK,
        cs.card_type
    FROM {{ ref("player_card_lnk") }} AS pc
    INNER JOIN {{ ref('cards_hub') }} AS cs ON cs.CARD_HK = pc.CARD_HK
)
SELECT
    cp.club_name,
    cp.player_name,
    SUM(
        CASE
            WHEN pc.card_type = 'Red Card' THEN 5
            WHEN pc.card_type = 'Yellow Card' THEN 1
            WHEN pc.card_type = 'Second Yellow' THEN 3
            ELSE 0
        END
    ) AS total_points,
    COUNT(CASE WHEN pc.card_type = 'Red Card' THEN 1 ELSE NULL END) AS red_cards_count,
    COUNT(CASE WHEN pc.card_type = 'Second Yellow' THEN 1 ELSE NULL END) AS second_yellow_cards_count,
    COUNT(CASE WHEN pc.card_type = 'Yellow Card' THEN 1 ELSE NULL END) AS yellow_cards_count
FROM club_player AS cp
LEFT JOIN player_card AS pc ON cp.PLAYER_HK = pc.PLAYER_HK
GROUP BY cp.player_name, cp.club_name
ORDER BY cp.club_name, total_points DESC
