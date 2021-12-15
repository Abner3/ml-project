-- To do: First group by AVG to get rid of duplicates.
--  Then do a sub-query and group by MAX to separate out the params into their own features without any NULLs

-- Take into account parameter_code for all the JOIN'ed tables.
-- Or else run the risk of averaging different parameter types

-- Number of unique param codes:
-- pm25: 1 | 88101
-- co: 1 | 42101
-- hap: 17 | 43218, 43502, 43503, 43505, 43802, 43803, 43804, 43815, 43817, 43818, 43824, 43829, 43830, 43831, 43843, 43860, 45201
-- no2: 1 | 42602
-- niox: 3 | 42600, 42601, 42603
-- o3: 1 | 44201
-- pm10: 1 | 81102
-- prsr: 1 | 64101
-- rh: 2 | 62103, 62201
-- so2: 1 | 42401
-- tmp: 1 | 62101
-- voc: 90 | LOL I'm not doing this one
-- ws: 2 | 61103, 61104

-- Full Query: 1059928 | +without hap: 8549450 | +without niox: 3140032

SELECT
  pm25.state_code,
  pm25.county_code,
  pm25.date_gmt,
  pm25.time_gmt,
  pm25.site_num,
  pm25.poc AS pm25_poc,
  pm25.latitude,
  pm25.longitude,
  pm25.method_code AS pm25_method_code,
  pm25.sample_measurement AS pm25_sample_amt,

  co.parameter_code AS co_parameter_code,
  AVG(co.sample_measurement) AS co_sample_amt,
  -- hap.parameter_code AS hap_parameter_code,
  -- AVG(hap.sample_measurement) AS hap_sample_amt,
  no2.parameter_code AS no2_parameter_code,
  AVG(no2.sample_measurement) AS no2_sample_amt,
  niox.parameter_code AS niox_parameter_code,
  AVG(niox.sample_measurement) AS niox_sample_amt,
  o3.parameter_code AS o3_parameter_code,
  AVG(o3.sample_measurement) AS o3_sample_amt,
  pm10.parameter_code AS pm10_parameter_code,
  AVG(pm10.sample_measurement) AS pm10_sample_amt,
  prsr.parameter_code AS prsr_parameter_code,
  AVG(prsr.sample_measurement) AS prsr_sample_amt,
  rh.parameter_code AS rh_parameter_code,
  AVG(rh.sample_measurement) AS rh_sample_amt,
  so2.parameter_code AS so2_parameter_code,
  AVG(so2.sample_measurement) AS so2_sample_amt,
  tmp.parameter_code AS tmp_parameter_code,
  AVG(tmp.sample_measurement) AS tmp_sample_amt,
  ws.parameter_code AS ws_parameter_code,
  AVG(ws.sample_measurement) AS ws_sample_amt,


FROM
  `bigquery-public-data.epa_historical_air_quality.pm25_frm_hourly_summary` AS pm25


JOIN
  `bigquery-public-data.epa_historical_air_quality.co_hourly_summary` AS co
ON
  co.state_code = pm25.state_code
  AND co.county_code = pm25.county_code
  AND co.date_gmt = pm25.date_gmt
  AND co.time_gmt = pm25.time_gmt
  AND ROUND(co.latitude,1) = ROUND(pm25.latitude,1)
  AND ROUND(co.longitude,1) = ROUND(pm25.longitude,1)

-- JOIN
--   `bigquery-public-data.epa_historical_air_quality.hap_hourly_summary` AS hap
-- ON
--   hap.state_code = pm25.state_code
--   AND hap.county_code = pm25.county_code
--   AND hap.date_gmt = pm25.date_gmt
--   AND hap.time_gmt = pm25.time_gmt
--   AND ROUND(hap.latitude,1) = ROUND(pm25.latitude,1)
--   AND ROUND(hap.longitude,1) = ROUND(pm25.longitude,1)

JOIN
  `bigquery-public-data.epa_historical_air_quality.no2_hourly_summary` AS no2
ON
  no2.state_code = pm25.state_code
  AND no2.county_code = pm25.county_code
  AND no2.date_gmt = pm25.date_gmt
  AND no2.time_gmt = pm25.time_gmt
  AND ROUND(no2.latitude,1) = ROUND(pm25.latitude,1)
  AND ROUND(no2.longitude,1) = ROUND(pm25.longitude,1)

JOIN
  `bigquery-public-data.epa_historical_air_quality.nonoxnoy_hourly_summary` AS niox -- nitrous oxide
ON
  niox.state_code = pm25.state_code
  AND niox.county_code = pm25.county_code
  AND niox.date_gmt = pm25.date_gmt
  AND niox.time_gmt = pm25.time_gmt
  AND ROUND(niox.latitude,1) = ROUND(pm25.latitude,1)
  AND ROUND(niox.longitude,1) = ROUND(pm25.longitude,1)

JOIN
  `bigquery-public-data.epa_historical_air_quality.o3_hourly_summary` AS o3
ON
  o3.state_code = pm25.state_code
  AND o3.county_code = pm25.county_code
  AND o3.date_gmt = pm25.date_gmt
  AND o3.time_gmt = pm25.time_gmt
  AND ROUND(o3.latitude,1) = ROUND(pm25.latitude,1)
  AND ROUND(o3.longitude,1) = ROUND(pm25.longitude,1)

JOIN
  `bigquery-public-data.epa_historical_air_quality.o3_hourly_summary` AS pm10
ON
  pm10.state_code = pm25.state_code
  AND pm10.county_code = pm25.county_code
  AND pm10.date_gmt = pm25.date_gmt
  AND pm10.time_gmt = pm25.time_gmt
  AND ROUND(pm10.latitude,1) = ROUND(pm25.latitude,1)
  AND ROUND(pm10.longitude,1) = ROUND(pm25.longitude,1)

JOIN
  `bigquery-public-data.epa_historical_air_quality.pressure_hourly_summary` AS prsr
ON
  prsr.state_code = pm25.state_code
  AND prsr.county_code = pm25.county_code
  AND prsr.date_gmt = pm25.date_gmt
  AND prsr.time_gmt = pm25.time_gmt
  AND ROUND(prsr.latitude,1) = ROUND(pm25.latitude,1)
  AND ROUND(prsr.longitude,1) = ROUND(pm25.longitude,1)

JOIN
  `bigquery-public-data.epa_historical_air_quality.rh_and_dp_hourly_summary` AS rh
ON
  rh.state_code = pm25.state_code
  AND rh.county_code = pm25.county_code
  AND rh.date_gmt = pm25.date_gmt
  AND rh.time_gmt = pm25.time_gmt
  AND ROUND(rh.latitude,1) = ROUND(pm25.latitude,1)
  AND ROUND(rh.longitude,1) = ROUND(pm25.longitude,1)

JOIN
  `bigquery-public-data.epa_historical_air_quality.so2_hourly_summary` AS so2
ON
  so2.state_code = pm25.state_code
  AND so2.county_code = pm25.county_code
  AND so2.date_gmt = pm25.date_gmt
  AND so2.time_gmt = pm25.time_gmt
  AND ROUND(so2.latitude,1) = ROUND(pm25.latitude,1)
  AND ROUND(so2.longitude,1) = ROUND(pm25.longitude,1)

JOIN
  `bigquery-public-data.epa_historical_air_quality.temperature_hourly_summary` AS tmp
ON
  tmp.state_code = pm25.state_code
  AND tmp.county_code = pm25.county_code
  AND tmp.date_gmt = pm25.date_gmt
  AND tmp.time_gmt = pm25.time_gmt
  AND ROUND(tmp.latitude,1) = ROUND(pm25.latitude,1)
  AND ROUND(tmp.longitude,1) = ROUND(pm25.longitude,1)

JOIN
  `bigquery-public-data.epa_historical_air_quality.wind_hourly_summary` AS ws
ON
  ws.state_code = pm25.state_code
  AND ws.county_code = pm25.county_code
  AND ws.date_gmt = pm25.date_gmt
  AND ws.time_gmt = pm25.time_gmt
  AND ROUND(ws.latitude,1) = ROUND(pm25.latitude,1)
  AND ROUND(ws.longitude,1) = ROUND(pm25.longitude,1)


WHERE
  pm25.qualifier IS NULL
  AND co.qualifier IS NULL
  AND so2.qualifier IS NULL
  AND no2.qualifier IS NULL
  AND o3.qualifier IS NULL
  -- AND hap.qualifier IS NULL
  AND niox.qualifier IS NULL
  AND pm10.qualifier IS NULL
  AND prsr.qualifier IS NULL
  AND rh.qualifier IS NULL
  AND tmp.qualifier IS NULL
  AND ws.qualifier IS NULL

GROUP BY
  pm25.state_code,
  pm25.county_code,
  pm25.date_gmt,
  pm25.time_gmt,
  pm25.site_num,
  pm25.poc,
  pm25.latitude,
  pm25.longitude,
  pm25.method_code,
  pm25.sample_measurement,
  co.parameter_code,
  -- hap.parameter_code,
  no2.parameter_code,
  niox.parameter_code,
  o3.parameter_code,
  pm10.parameter_code,
  prsr.parameter_code,
  rh.parameter_code,
  so2.parameter_code,
  tmp.parameter_code,
  ws.parameter_code

ORDER BY
  pm25.state_code,
  pm25.county_code,
  pm25.date_gmt,
  pm25.time_gmt ASC