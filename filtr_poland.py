import pandas as pd
from datetime import datetime, timedelta
import os
import time
import helper_pol as h
from dotenv import load_dotenv

load_dotenv('/home/ca-polandsys/.env')



country_code = 'PL'

h.connect_to_rdb()
query = r"SELECT min(max_report_date) AS max_report_date FROM stats.vw_partner_consumer_data_loads_by_country WHERE country_code = '"+country_code+"' AND fact_table IN ('spotify.fact_streams','spotify.fact_streams_orchard')"
spotify_max_date_df = h.query_sql(query)

spotify_max_date = datetime.today()

try:
    spotify_max_date = spotify_max_date_df['max_report_date'].loc[0]

except:
    next
    

query = r"SELECT min(max_report_date) AS max_report_date FROM stats.vw_partner_consumer_data_loads_by_country WHERE country_code = '"+country_code+"' AND fact_table IN ('tiktok.fact_views')"
tiktok_max_date_df = h.query_sql(query)

tiktok_max_date = datetime.today()

try:
    tiktok_max_date = tiktok_max_date_df['max_report_date'].loc[0]

except:
    next
    
    
    
with open("sql/tracks_sme.sql", "r") as file:
    tracks_sme_sql = file.read()
    
    
tracks_sme_sql = tracks_sme_sql.replace('2024-01-07',spotify_max_date.strftime("%Y-%m-%d"))
tracks_sme_sql = tracks_sme_sql.replace('_COUNTRY_CODE_',country_code)
tracks_sme_df = h.query_sql(tracks_sme_sql)
tracks_sme_df['feed'] = 'SME'


with open("sql/tracks_orchard.sql", "r") as file:
    tracks_orch_sql = file.read()

tracks_orch_sql = tracks_orch_sql.replace('2024-01-07',spotify_max_date.strftime("%Y-%m-%d"))
tracks_orch_sql = tracks_orch_sql.replace('_COUNTRY_CODE_',country_code)
tracks_orch_df = h.query_sql(tracks_orch_sql)
tracks_orch_df['feed'] = 'ORCHARD'



with open("sql/tiktok_sony.sql", "r") as file:
    tiktok_sme_sql = file.read()
    
    
tiktok_sme_sql = tiktok_sme_sql.replace('2024-01-07',tiktok_max_date.strftime("%Y-%m-%d"))
tiktok_sme_sql = tiktok_sme_sql.replace('_COUNTRY_CODE_',country_code)
tiktok_sme_df = h.query_sql(tiktok_sme_sql)
tiktok_sme_df['feed'] = 'SME'

with open("sql/tiktok_orchard.sql", "r") as file:
    tiktok_orch_sql = file.read()
    
    
tiktok_orch_sql = tiktok_orch_sql.replace('2024-01-07',tiktok_max_date.strftime("%Y-%m-%d"))
tiktok_orch_sql = tiktok_orch_sql.replace('_COUNTRY_CODE_',country_code)
tiktok_orch_df = h.query_sql(tiktok_orch_sql)
tiktok_orch_df['feed'] = 'ORCHARD'

tiktok_df = pd.concat([tiktok_sme_df, tiktok_orch_df], ignore_index=True)




tracks_df = pd.concat([tracks_sme_df, tracks_orch_df], ignore_index=True)

df_metadata = tracks_df.drop_duplicates(subset='track_isrc')
df_metadata = df_metadata.drop(columns=['segment_name','segment_streams'])


combined_isrcs = ','.join(["'{}'".format(isrc) for isrc in df_metadata['track_isrc']])

with open("sql/metadane_release_date.sql", "r") as file:
    metadane_release_date_sql = file.read()

metadane_release_date_sql = metadane_release_date_sql.replace('_ISRC_CD_',combined_isrcs)
metadane_release_date = h.query_sql(metadane_release_date_sql)
metadane_release_date['release_date'] = pd.to_datetime(metadane_release_date['release_date'])

min_release_dates_df = metadane_release_date.groupby('track_isrc')['release_date'].min().reset_index()
min_release_dates_df = pd.DataFrame(min_release_dates_df, columns=['track_isrc', 'release_date'])

df_metadata = pd.merge(df_metadata, min_release_dates_df, on='track_isrc', how='left')


pivot_df = pd.pivot_table(tracks_df, values='segment_streams', index=['track_isrc'],
                          columns='segment_name', aggfunc='sum', fill_value=0)
genres_df = pivot_df.div(pivot_df.sum(axis=1), axis=0) * 100
genres_df.reset_index(inplace=True)

none_df = (tracks_df.groupby('track_isrc')
                       .apply(lambda x: x.loc[x['segment_name'].isna(), 'segment_streams'].sum() / x['segment_streams'].sum())
                       .reset_index(name='share_of_none_values'))

genres_df = pd.merge(genres_df, none_df, on='track_isrc', how='outer')


genres_df = pd.merge(df_metadata, genres_df, on='track_isrc', how='inner')
genres_df = genres_df.sort_values(by='total_streams', ascending=False)
genres_df.reset_index(drop=True,inplace=True)



#Top Hity – Top150 SME+O, ALL

playlist_id = '7Le3oCnCpgDO0etNcbyTeM'
h.refresh_spotify_conn()

df_playlist = genres_df[0:150]

if len(df_playlist) >= 0:
    h.spotify_remove_all_tracks_from_playlist(playlist_id)
    h.spotify_update_playlist_description(playlist_id,'Dane na dzien '+spotify_max_date.strftime("%Y-%m-%d"))
    group_size = 100
    track_uri_groups = [df_playlist['_track_uri'].iloc[i:i+group_size].to_list() for i in range(0, len(df_playlist), group_size)]

    for i in range(len(track_uri_groups)-1, -1, -1):
        track_uri = track_uri_groups[i]
        h.spotify_add_tracks(playlist_id,track_uri)
        
        
#Top Hity – Top150 SME+O, ALL

playlist_id = '7Le3oCnCpgDO0etNcbyTeM'

df_playlist = genres_df[0:150]

if len(df_playlist) >= 0:
    h.spotify_remove_all_tracks_from_playlist(playlist_id)
    h.spotify_update_playlist_description(playlist_id,'Dane na dzien '+spotify_max_date.strftime("%Y-%m-%d"))
    group_size = 100
    track_uri_groups = [df_playlist['_track_uri'].iloc[i:i+group_size].to_list() for i in range(0, len(df_playlist), group_size)]

    for i in range(len(track_uri_groups)-1, -1, -1):
        track_uri = track_uri_groups[i]
        h.spotify_add_tracks(playlist_id,track_uri)
        
        
#Rap hity – Top150 SME+O, ALL

playlist_id = '5QqmWfetEZ7IfMvyJycezG'

df_playlist = genres_df[(genres_df['rap']>75.0) | genres_df['_track_uri'].str.contains('rap|hip-hop', case=False, regex=True)][0:150]

if len(df_playlist) >= 0:
    h.spotify_remove_all_tracks_from_playlist(playlist_id)
    h.spotify_update_playlist_description(playlist_id,'Dane na dzien '+spotify_max_date.strftime("%Y-%m-%d"))
    group_size = 100
    track_uri_groups = [df_playlist['_track_uri'].iloc[i:i+group_size].to_list() for i in range(0, len(df_playlist), group_size)]

    for i in range(len(track_uri_groups)-1, -1, -1):
        track_uri = track_uri_groups[i]
        h.spotify_add_tracks(playlist_id,track_uri)
        
        
#Gram w grę – Top150 SME+O, ALL

playlist_id = '4HIyk5bEmoPJVaXwhN3oJu'

df_playlist = genres_df[(genres_df['edm']>25.0) | ((genres_df['rap']>55.0) & (genres_df['edm']>1.0)) | genres_df['_track_uri'].str.contains('rap|hip-hop|electro|edm', case=False, regex=True)][0:150]

if len(df_playlist) >= 0:
    h.spotify_remove_all_tracks_from_playlist(playlist_id)
    h.spotify_update_playlist_description(playlist_id,'Dane na dzien '+spotify_max_date.strftime("%Y-%m-%d"))
    group_size = 100
    track_uri_groups = [df_playlist['_track_uri'].iloc[i:i+group_size].to_list() for i in range(0, len(df_playlist), group_size)]

    for i in range(len(track_uri_groups)-1, -1, -1):
        track_uri = track_uri_groups[i]
        h.spotify_add_tracks(playlist_id,track_uri)
        
        
#PPP – Top150 SME+O, ALL

playlist_id = '0Tt1VCYMzvK0KtKySr18So'
h.refresh_spotify_conn()

df_playlist = genres_df[(genres_df['edm']>30.0) | (genres_df['_track_uri'].str.contains('electro|edm', case=False, regex=True))][0:150]

if len(df_playlist) >= 0:
    h.spotify_remove_all_tracks_from_playlist(playlist_id)
    h.spotify_update_playlist_description(playlist_id,'Dane na dzien '+spotify_max_date.strftime("%Y-%m-%d"))
    group_size = 100
    track_uri_groups = [df_playlist['_track_uri'].iloc[i:i+group_size].to_list() for i in range(0, len(df_playlist), group_size)]

    for i in range(len(track_uri_groups)-1, -1, -1):
        track_uri = track_uri_groups[i]
        h.spotify_add_tracks(playlist_id,track_uri)
        
        
#Top Hity – Top150 SME+O, ALL

playlist_id = '5wGiR9VtUNlfSGaqa7Wniw'

cutoff_date = datetime.now() - timedelta(days=56)
df_playlist = genres_df[genres_df['release_date'] <= cutoff_date][0:150]

if len(df_playlist) >= 0:
    h.spotify_remove_all_tracks_from_playlist(playlist_id)
    h.spotify_update_playlist_description(playlist_id,'Dane na dzien '+spotify_max_date.strftime("%Y-%m-%d"))
    group_size = 100
    track_uri_groups = [df_playlist['_track_uri'].iloc[i:i+group_size].to_list() for i in range(0, len(df_playlist), group_size)]

    for i in range(len(track_uri_groups)-1, -1, -1):
        track_uri = track_uri_groups[i]
        h.spotify_add_tracks(playlist_id,track_uri)
        
        
import string
from math import sqrt

def preprocess_text(text):
    # Convert to lowercase
    text = text.lower()
    # Remove punctuation
    text = text.translate(str.maketrans("", "", string.punctuation))
    return text

def calculate_cosine_similarity(str1, str2):
    str1 = preprocess_text(str1)
    str2 = preprocess_text(str2)

    # Create a set of unique characters
    all_chars = set(str1 + str2)

    # Calculate character frequency (CF) vectors
    cf_vector1 = [str1.count(char) for char in all_chars]
    cf_vector2 = [str2.count(char) for char in all_chars]

    # Calculate the dot product of the CF vectors
    dot_product = sum(cf1 * cf2 for cf1, cf2 in zip(cf_vector1, cf_vector2))

    # Calculate the magnitude of each CF vector
    magnitude1 = sqrt(sum(cf**2 for cf in cf_vector1))
    magnitude2 = sqrt(sum(cf**2 for cf in cf_vector2))

    # Calculate cosine similarity
    similarity = dot_product / (magnitude1 * magnitude2) if magnitude1 > 0 and magnitude2 > 0 else 0.0

    return similarity

def fuzzy_match(row, threshold_name, threshold_artist):
    similarity_name = calculate_cosine_similarity(row['_track_name'], row['_track_name_tt'])
    similarity_artist = calculate_cosine_similarity(row['_track_artist'], row['_artist_name_tt'])

    return (similarity_name >= threshold_name and similarity_artist >= threshold_artist) or (row['track_isrc']==row['isrc_cd'])

threshold_name = 0.9  # Example threshold for _track_name
threshold_artist = 0.9  # Example threshold for _track_artist

cartesian_df = pd.merge(genres_df[0:500].assign(key=1), tiktok_df.assign(key=1), on='key').drop('key', axis=1)

matched_pairs = cartesian_df[cartesian_df.apply(lambda row: fuzzy_match(row, threshold_name, threshold_artist), axis=1)]



#TOP VIRAL

playlist_id = '1Ubg6u4Z7zRRvqrjG1dMnw'

df_no_duplicates = matched_pairs.drop_duplicates(subset='_track_uri')

df_playlist = df_no_duplicates.sort_values(by='tt_creations', ascending=False)[0:150]


if len(df_playlist) >= 0:
    h.spotify_remove_all_tracks_from_playlist(playlist_id)
    h.spotify_update_playlist_description(playlist_id,'Dane na dzien '+spotify_max_date.strftime("%Y-%m-%d"))
    group_size = 100
    track_uri_groups = [df_playlist['_track_uri'].iloc[i:i+group_size].to_list() for i in range(0, len(df_playlist), group_size)]

    for i in range(len(track_uri_groups)-1, -1, -1):
        track_uri = track_uri_groups[i]
        h.spotify_add_tracks(playlist_id,track_uri)
        
        
country_code = 'CZ'

h.connect_to_rdb()
query = r"SELECT min(max_report_date) AS max_report_date FROM stats.vw_partner_consumer_data_loads_by_country WHERE country_code = '"+country_code+"' AND fact_table IN ('spotify.fact_streams','spotify.fact_streams_orchard')"
spotify_max_date_df = h.query_sql(query)

spotify_max_date = datetime.today()

try:
    spotify_max_date = spotify_max_date_df['max_report_date'].loc[0]

except:
    next

with open("sql/tracks_sme.sql", "r") as file:
    tracks_sme_sql = file.read()
    
    
tracks_sme_sql = tracks_sme_sql.replace('2024-01-07',spotify_max_date.strftime("%Y-%m-%d"))
tracks_sme_sql = tracks_sme_sql.replace('_COUNTRY_CODE_',country_code)
tracks_sme_df = h.query_sql(tracks_sme_sql)
tracks_sme_df['feed'] = 'SME'


with open("sql/tracks_orchard.sql", "r") as file:
    tracks_orch_sql = file.read()

tracks_orch_sql = tracks_orch_sql.replace('2024-01-07',spotify_max_date.strftime("%Y-%m-%d"))
tracks_orch_sql = tracks_orch_sql.replace('_COUNTRY_CODE_',country_code)
tracks_orch_df = h.query_sql(tracks_orch_sql)
tracks_orch_df['feed'] = 'ORCHARD'


tracks_df = pd.concat([tracks_sme_df, tracks_orch_df], ignore_index=True)

df_metadata = tracks_df.drop_duplicates(subset='track_isrc')
df_metadata = df_metadata.drop(columns=['segment_name','segment_streams'])



combined_isrcs = ','.join(["'{}'".format(isrc) for isrc in df_metadata['track_isrc']])

with open("sql/metadane_release_date.sql", "r") as file:
    metadane_release_date_sql = file.read()

metadane_release_date_sql = metadane_release_date_sql.replace('_ISRC_CD_',combined_isrcs)
metadane_release_date = h.query_sql(metadane_release_date_sql)
metadane_release_date['release_date'] = pd.to_datetime(metadane_release_date['release_date'])

min_release_dates_df = metadane_release_date.groupby('track_isrc')['release_date'].min().reset_index()
min_release_dates_df = pd.DataFrame(min_release_dates_df, columns=['track_isrc', 'release_date'])

df_metadata = pd.merge(df_metadata, min_release_dates_df, on='track_isrc', how='left')


pivot_df = pd.pivot_table(tracks_df, values='segment_streams', index=['track_isrc'],
                          columns='segment_name', aggfunc='sum', fill_value=0)
genres_df = pivot_df.div(pivot_df.sum(axis=1), axis=0) * 100
genres_df.reset_index(inplace=True)

none_df = (tracks_df.groupby('track_isrc')
                       .apply(lambda x: x.loc[x['segment_name'].isna(), 'segment_streams'].sum() / x['segment_streams'].sum())
                       .reset_index(name='share_of_none_values'))

genres_df = pd.merge(genres_df, none_df, on='track_isrc', how='outer')


genres_df = pd.merge(df_metadata, genres_df, on='track_isrc', how='inner')
genres_df = genres_df.sort_values(by='total_streams', ascending=False)
genres_df.reset_index(drop=True,inplace=True)



#Top Hity – Top150 SME+O, ALL

playlist_id = '1e3G6ZlcQrqjoomu3sGZ0m'

df_playlist = genres_df[0:150]

if len(df_playlist) >= 0:
    h.spotify_remove_all_tracks_from_playlist(playlist_id)
    h.spotify_update_playlist_description(playlist_id,'Dane na dzien '+spotify_max_date.strftime("%Y-%m-%d"))
    group_size = 100
    track_uri_groups = [df_playlist['_track_uri'].iloc[i:i+group_size].to_list() for i in range(0, len(df_playlist), group_size)]

    for i in range(len(track_uri_groups)-1, -1, -1):
        track_uri = track_uri_groups[i]
        h.spotify_add_tracks(playlist_id,track_uri)
        
        
        
country_code = 'SK'

h.connect_to_rdb()
query = r"SELECT min(max_report_date) AS max_report_date FROM stats.vw_partner_consumer_data_loads_by_country WHERE country_code = '"+country_code+"' AND fact_table IN ('spotify.fact_streams','spotify.fact_streams_orchard')"
spotify_max_date_df = h.query_sql(query)

spotify_max_date = datetime.today()

try:
    spotify_max_date = spotify_max_date_df['max_report_date'].loc[0]

except:
    next

with open("sql/tracks_sme.sql", "r") as file:
    tracks_sme_sql = file.read()
    
    
tracks_sme_sql = tracks_sme_sql.replace('2024-01-07',spotify_max_date.strftime("%Y-%m-%d"))
tracks_sme_sql = tracks_sme_sql.replace('_COUNTRY_CODE_',country_code)
tracks_sme_df = h.query_sql(tracks_sme_sql)
tracks_sme_df['feed'] = 'SME'


with open("sql/tracks_orchard.sql", "r") as file:
    tracks_orch_sql = file.read()

tracks_orch_sql = tracks_orch_sql.replace('2024-01-07',spotify_max_date.strftime("%Y-%m-%d"))
tracks_orch_sql = tracks_orch_sql.replace('_COUNTRY_CODE_',country_code)
tracks_orch_df = h.query_sql(tracks_orch_sql)
tracks_orch_df['feed'] = 'ORCHARD'


tracks_df = pd.concat([tracks_sme_df, tracks_orch_df], ignore_index=True)

df_metadata = tracks_df.drop_duplicates(subset='track_isrc')
df_metadata = df_metadata.drop(columns=['segment_name','segment_streams'])



combined_isrcs = ','.join(["'{}'".format(isrc) for isrc in df_metadata['track_isrc']])

with open("sql/metadane_release_date.sql", "r") as file:
    metadane_release_date_sql = file.read()

metadane_release_date_sql = metadane_release_date_sql.replace('_ISRC_CD_',combined_isrcs)
metadane_release_date = h.query_sql(metadane_release_date_sql)
metadane_release_date['release_date'] = pd.to_datetime(metadane_release_date['release_date'])

min_release_dates_df = metadane_release_date.groupby('track_isrc')['release_date'].min().reset_index()
min_release_dates_df = pd.DataFrame(min_release_dates_df, columns=['track_isrc', 'release_date'])

df_metadata = pd.merge(df_metadata, min_release_dates_df, on='track_isrc', how='left')


pivot_df = pd.pivot_table(tracks_df, values='segment_streams', index=['track_isrc'],
                          columns='segment_name', aggfunc='sum', fill_value=0)
genres_df = pivot_df.div(pivot_df.sum(axis=1), axis=0) * 100
genres_df.reset_index(inplace=True)

none_df = (tracks_df.groupby('track_isrc')
                       .apply(lambda x: x.loc[x['segment_name'].isna(), 'segment_streams'].sum() / x['segment_streams'].sum())
                       .reset_index(name='share_of_none_values'))

genres_df = pd.merge(genres_df, none_df, on='track_isrc', how='outer')


genres_df = pd.merge(df_metadata, genres_df, on='track_isrc', how='inner')
genres_df = genres_df.sort_values(by='total_streams', ascending=False)
genres_df.reset_index(drop=True,inplace=True)



#Top Hity – Top150 SME+O, ALL

playlist_id = '5kabOALXCjHfEps1U6dbPv'

df_playlist = genres_df[0:150]

if len(df_playlist) >= 0:
    h.spotify_remove_all_tracks_from_playlist(playlist_id)
    h.spotify_update_playlist_description(playlist_id,'Dane na dzien '+spotify_max_date.strftime("%Y-%m-%d"))
    group_size = 100
    track_uri_groups = [df_playlist['_track_uri'].iloc[i:i+group_size].to_list() for i in range(0, len(df_playlist), group_size)]

    for i in range(len(track_uri_groups)-1, -1, -1):
        track_uri = track_uri_groups[i]
        h.spotify_add_tracks(playlist_id,track_uri)
        
        
