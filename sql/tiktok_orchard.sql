SELECT 
dpi.isrc_cd,
min(dpi.artist_name) AS _artist_name_tt,
min(dpi.track_title) AS _track_name_tt,
sum(fv.creations) AS tt_creations
FROM tiktok.fact_views_orchard fv 
JOIN tiktok.dim_partner_info dpi USING (partner_info_key)
WHERE fv.country_code = '_COUNTRY_CODE_' AND fv.report_date = '2024-01-07'::date 
GROUP BY 1 ORDER BY tt_creations DESC 
LIMIT 50