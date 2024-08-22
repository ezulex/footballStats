DROP EXTERNAL TABLE IF EXISTS stage.matches_ext CASCADE;
CREATE EXTERNAL TABLE stage.matches_ext(
    match_id bigint,
    match_date date,
    home_score bigint,
    away_score bigint,
    last_updated date,
    match_week bigint,
    competition_competition_id bigint,
    season_season_id bigint,
    home_team_home_team_id bigint,
    home_team_home_team_name varchar(100),
    away_team_away_team_id bigint,
    away_team_away_team_name varchar(100),
    stadium_id bigint,
    stadium_name varchar(100),
    referee_id bigint,
    referee_name varchar(100),
    load_date text
)
LOCATION (
	'pxf://{{ params.bucket }}/{{ ds_nodash }}/matches_df.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';