DROP EXTERNAL TABLE IF EXISTS stage.lineups_ext CASCADE;
CREATE EXTERNAL TABLE stage.lineups_ext(
    match_id bigint,
    teams_team_id bigint,
    player_id bigint,
    player_name varchar(100),
    player_nickname varchar(100),
    jersey_number bigint,
    country_id bigint,
    country_name varchar(100),
    load_date varchar(8)
)
LOCATION (
	'pxf://{{ params.bucket }}/{{ ds_nodash }}/lineups_df.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';