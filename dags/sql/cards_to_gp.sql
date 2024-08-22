DROP EXTERNAL TABLE IF EXISTS stage.cards_ext CASCADE;
CREATE EXTERNAL TABLE stage.cards_ext(
    time varchar(6),
    card_type varchar(50),
    reason varchar(100),
    period bigint,
    match_id bigint,
    teams_lineup_player_id bigint,
    load_date varchar(8)
)
LOCATION (
	'pxf://{{ params.bucket }}/{{ ds_nodash }}/cards_df.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';