-- Gets PM 2.5 info from table "pm25_frm_hourly_summary" from EPA dataset
#standardSQL
SELECT
    epa.state_code,
    epa.county_code,
    epa.site_num,
    epa.poc,
    epa.latitude,
    epa.longitude,
    epa.date_gmt,
    epa.time_gmt,
    epa.sample_measurement,
    epa.mdl,
    epa.method_code
FROM
    `bigquery-public-data.epa_historical_air_quality.pm25_frm_hourly_summary` AS epa
WHERE
    epa.qualifier IS NULL
ORDER BY
    epa.date_gmt,
    epa.time_gmt
