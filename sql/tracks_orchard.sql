WITH spotify_top AS (
SELECT dpi.track_isrc,
min(dpi.track_uri) AS _track_uri,
min(dpi.track_artists) AS _track_artist,
min(dpi.track_name) AS _track_name,
sum(fs2.num_streams) AS total_streams
FROM spotify.fact_streams_orchard fs2 
JOIN spotify.dim_partner_info dpi USING (partner_info_key)
WHERE fs2.report_date = '2024-01-07'::date AND fs2.country_code_stream = '_COUNTRY_CODE_'
GROUP BY 1
ORDER BY total_streams DESC 
LIMIT 500),

orchard_lc AS (SELECT vrtd.isrc AS track_isrc,
min(genre_name) AS _primary_genre
FROM common_orchard.vw_release_track_details vrtd GROUP BY 1),

segments as (SELECT dpi.track_isrc,
scs.segment_name,
min(_track_uri) AS _track_uri,
min(_track_artist) AS _track_artist,
min(_track_name) AS _track_name,
sum(fs2.num_streams) as segment_streams,
min(_primary_genre) AS _primary_genre,
max(total_streams) AS total_streams
FROM spotify.fact_streams_orchard fs2 
JOIN spotify.dim_partner_info dpi USING (partner_info_key)
JOIN spotify_top USING (track_isrc)
LEFT JOIN sandbox.spotify_consumers_segments scs USING (consumer_key,country_code)
LEFT JOIN orchard_lc ol USING (track_isrc)
WHERE fs2.report_date = '2024-01-07'::date AND fs2.country_code_stream = '_COUNTRY_CODE_'
GROUP BY 1,2),

covers_raw AS (SELECT 
dpi.track_isrc,
dpi.track_uri,
ROW_NUMBER() OVER (PARTITION BY dpi.track_isrc ORDER BY sum(aph.num_streams) DESC) AS cover_no
FROM spotify.fact_streams_orchard aph 
JOIN spotify.dim_partner_info dpi USING (partner_info_key)
WHERE aph.country_code = 'PL' AND aph.source_type_key IN (487,416) AND aph.report_date >= CURRENT_DATE - 14
GROUP BY 1,2),

covers AS (SELECT track_isrc, track_uri AS track_uri_fs FROM covers_raw WHERE cover_no = 1)

SELECT 
track_isrc,
segment_name,
NVL(track_uri_fs,_track_uri) AS _track_uri,
_track_artist,
_track_name,
_primary_genre,
segment_streams,
total_streams
FROM segments
LEFT JOIN covers USING (track_isrc)