DROP EXTERNAL TABLE IF EXISTS stage.competitions_ext CASCADE;
CREATE EXTERNAL TABLE stage.competitions_ext(
    competition_id bigint,
    season_id bigint,
    country_name varchar(100),
    competition_name varchar(100),
    competition_gender varchar(10),
    competition_youth bool,
    competition_international bool,
    season_name varchar(100),
    load_date varchar(8)
)
LOCATION (
	'pxf://{{ params.bucket }}/{{ ds_nodash }}/competitions_df.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';