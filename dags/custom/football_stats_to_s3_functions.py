import io
import boto3
import warnings
import pandas as pd
import pyarrow as pa
from botocore.exceptions import ClientError
from pyarrow import parquet as pq
from statsbombpy import sb


def get_s3_conn(login, secret, endpoint):
    s3 = boto3.resource(service_name='s3',
                        endpoint_url=endpoint,
                        aws_access_key_id=login,
                        aws_secret_access_key=secret)
    return s3


def put_parquet(f_df, f_filename, f_bucket, f_dt):
    f_buffer = io.BytesIO()
    f_table = pa.Table.from_pandas(f_df)
    pq.write_table(f_table, f_buffer)
    f_buffer.seek(0)
    f_object = f_bucket.Object(key=f'{f_dt}/{f_filename}.parquet')
    f_object.put(Body=f_buffer)
    print(f"File {f_filename}.parquet uploaded to bucket {f_bucket.name}")


def create_bucket(bucket_name, login, secret, endpoint, dt) -> None:
    s3 = get_s3_conn(login, secret, endpoint)

    try:
        s3.Bucket(name=bucket_name).create()
        print(f'Bucket "{bucket_name}" created successfully.')
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f'Bucket "{bucket_name}" already exists.')
        else:
            print(f'Creation bucket error: {e}')


def load_competitions_to_s3(bucket_name, login, secret, endpoint, dt):
    s3 = get_s3_conn(login, secret, endpoint)
    bucket = s3.Bucket(name=bucket_name)
    competitions_df = sb.competitions()
    competitions_df['load_date'] = dt
    competitions_df = competitions_df[
        ['competition_id', 'season_id', 'country_name', 'competition_name', 'competition_gender', 'competition_youth',
         'competition_international', 'season_name', 'load_date']]
    put_parquet(competitions_df, 'competitions_df', bucket, dt)


def load_matches_to_s3(bucket_name, login, secret, endpoint, dt):
    s3 = get_s3_conn(login, secret, endpoint)
    bucket = s3.Bucket(name=bucket_name)
    matches = sb.matches(competition_id=9, season_id=281, fmt=dict)
    matches_df = pd.json_normalize(matches.values(), sep='_')
    matches_df['load_date'] = dt
    matches_df = matches_df[['match_id', 'match_date', 'home_score', 'away_score', 'last_updated', 'match_week',
                             'competition_competition_id', 'season_season_id', 'home_team_home_team_id',
                             'home_team_home_team_name', 'away_team_away_team_id', 'away_team_away_team_name',
                             'stadium_id', 'stadium_name', 'referee_id', 'referee_name', 'load_date']]
    put_parquet(matches_df, 'matches_df', bucket, dt)


def load_managers_to_s3(bucket_name, login, secret, endpoint, dt):
    s3 = get_s3_conn(login, secret, endpoint)
    bucket = s3.Bucket(name=bucket_name)
    matches = sb.matches(competition_id=9, season_id=281, fmt=dict)

    home_managers_df = pd.json_normalize(matches.values(), record_path=['home_team', 'managers'],
                                         meta=[['home_team', 'home_team_id'], ['match_date']], sep='_')
    away_managers_df = pd.json_normalize(matches.values(), record_path=['away_team', 'managers'],
                                         meta=[['away_team', 'away_team_id'], ['match_date']], sep='_')
    managers_df = pd.concat([home_managers_df, away_managers_df], ignore_index=True)
    managers_df['home_team_home_team_id'] = managers_df['home_team_home_team_id'].fillna(
        managers_df['away_team_away_team_id'])
    managers_df = managers_df.drop(columns=['away_team_away_team_id'])
    managers_df = managers_df.rename(columns={'home_team_home_team_id': 'team_id'})
    managers_df['load_date'] = dt
    managers_df = managers_df[
        ['id', 'name', 'nickname', 'dob', 'country_id', 'country_name', 'team_id', 'match_date', 'load_date']]
    put_parquet(managers_df, 'managers_df', bucket, dt)


def load_lineups_to_s3(bucket_name, login, secret, endpoint, dt):
    s3 = get_s3_conn(login, secret, endpoint)
    bucket = s3.Bucket(name=bucket_name)
    matches = sb.matches(competition_id=9, season_id=281, fmt=dict)

    lineups_list = []
    for match in matches.keys():
        sb.lineups(match_id=match, fmt=dict)
        lineup = {}
        lineup['match'] = match
        for team in sb.lineups(match_id=match, fmt=dict).values():
            lineup['teams'] = team
        lineups_list.append(lineup)
    lineups_df = pd.json_normalize(lineups_list, record_path=['teams', 'lineup'],
                                   meta=[['teams', 'team_id'], ['match']], sep='_')
    lineups_df = lineups_df.rename(columns={'match': 'match_id'})
    lineups_df['load_date'] = dt
    lineups_df = lineups_df[
        ['match_id', 'teams_team_id', 'player_id', 'player_name', 'player_nickname', 'jersey_number', 'country_id',
         'country_name', 'load_date']]
    put_parquet(lineups_df, 'lineups_df', bucket, dt)


def load_cards_to_s3(bucket_name, login, secret, endpoint, dt):
    s3 = get_s3_conn(login, secret, endpoint)
    bucket = s3.Bucket(name=bucket_name)

    matches = sb.matches(competition_id=9, season_id=281, fmt=dict)
    lineups_list = []
    for match in matches.keys():
        sb.lineups(match_id=match, fmt=dict)
        lineup = {}
        lineup['match'] = match
        for team in sb.lineups(match_id=match, fmt=dict).values():
            lineup['teams'] = team
        lineups_list.append(lineup)

    cards_df = pd.json_normalize(lineups_list, record_path=['teams', 'lineup', 'cards'],
                                 meta=[['teams', 'lineup', 'player_id'], ['match']], sep='_')
    cards_df = cards_df.rename(columns={'match': 'match_id'})
    cards_df['load_date'] = dt
    cards_df = cards_df[['time', 'card_type', 'reason', 'period', 'match_id', 'teams_lineup_player_id', 'load_date']]
    put_parquet(cards_df, 'cards_df', bucket, dt)


def load_positions_to_s3(bucket_name, login, secret, endpoint, dt):
    s3 = get_s3_conn(login, secret, endpoint)
    bucket = s3.Bucket(name=bucket_name)

    matches = sb.matches(competition_id=9, season_id=281, fmt=dict)
    lineups_list = []
    for match in matches.keys():
        sb.lineups(match_id=match, fmt=dict)
        lineup = {}
        lineup['match'] = match
        for team in sb.lineups(match_id=match, fmt=dict).values():
            lineup['teams'] = team
        lineups_list.append(lineup)

    positions_df = pd.json_normalize(lineups_list, record_path=['teams', 'lineup', 'positions'],
                                     meta=[['teams', 'lineup', 'player_id'], ['match']], sep='_')
    positions_df = positions_df.rename(columns={'match': 'match_id'})
    positions_df['load_date'] = dt
    positions_df = positions_df[
        ['position_id', 'position', 'from', 'to', 'from_period', 'to_period', 'start_reason', 'end_reason', 'match_id',
         'teams_lineup_player_id', 'load_date']]
    positions_df = positions_df.rename(columns={'from': 'fr_om', 'to': 't_o'})
    put_parquet(positions_df, 'positions_df', bucket, dt)
