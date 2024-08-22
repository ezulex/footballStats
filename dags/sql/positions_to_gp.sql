DROP EXTERNAL TABLE IF EXISTS stage.positions_ext CASCADE;
CREATE EXTERNAL TABLE stage.positions_ext(
    position_id bigint,
    position varchar(50),
    fr_om varchar(6),
    t_o varchar(6),
    from_period bigint,
    to_period float8,
    start_reason varchar(50),
    end_reason varchar(50),
    match_id bigint,
    teams_lineup_player_id bigint,
    load_date varchar(8)
)
LOCATION (
	'pxf://{{ params.bucket }}/{{ ds_nodash }}/positions_df.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';