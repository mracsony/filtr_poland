SELECT s.isrc AS track_isrc, 
min(sa.release_date) AS release_date 
FROM chartmetric_raw.spotify s 
JOIN chartmetric_raw.spotify_album sa USING (spotify_album_id)
WHERE s.isrc IN (_ISRC_CD_)
GROUP BY 1
UNION ALL
SELECT dp.isrc_cd AS track_isrc,
min(dpr.release_date) AS release_date
FROM common.dim_products dp 
JOIN common.dim_product_releases dpr USING (product_key)
WHERE dp.isrc_cd IN (_ISRC_CD_)
GROUP BY 1
UNION ALL
SELECT vrtd.isrc AS track_isrc,
min(release_date) AS release_date
FROM common_orchard.vw_release_track_details vrtd 
WHERE vrtd.isrc IN (_ISRC_CD_)
GROUP BY 1