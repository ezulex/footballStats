DROP EXTERNAL TABLE IF EXISTS stage.managers_ext CASCADE;
CREATE EXTERNAL TABLE stage.managers_ext(
    id bigint,
    name varchar(100),
    nickname varchar(100),
    dob date,
    country_id bigint,
    country_name varchar(100),
    team_id bigint,
    match_date date,
    load_date text
)
LOCATION (
	'pxf://{{ params.bucket }}/{{ ds_nodash }}/managers_df.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';