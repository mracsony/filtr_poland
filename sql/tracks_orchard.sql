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
FROM common_orchard.vw_release_track_details vrtd GROUP BY 1)

SELECT dpi.track_isrc,
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
GROUP BY 1,2