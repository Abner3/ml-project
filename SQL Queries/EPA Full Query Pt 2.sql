SELECT
  *
FROM (
  SELECT
    t.state_code,
    t.county_code,
    t.date_gmt,
    t.time_gmt,
    t.latitude,
    t.longitude,
    t.pm25_sample_amt,
    MAX(niox_feat_1) AS niox_feat_1_max,
    MAX(niox_feat_2) AS niox_feat_2_max,
    MAX(niox_feat_3) AS niox_feat_3_max,
    MAX(rh_feat_1) AS rh_feat_1_max,
    MAX(rh_feat_2) AS rh_feat_2_max,
    MAX(ws_feat_1) AS ws_feat_1_max,
    MAX(ws_feat_2) AS ws_feat_2_max
  FROM (
    SELECT
      big.state_code,
      big.county_code,
      big.date_gmt,
      big.time_gmt,
      big.site_num,
      big.pm25_poc,
      big.latitude,
      big.longitude,
      big.pm25_method_code,
      big.pm25_sample_amt,
      -- niox
      CASE big.niox_parameter_code
        WHEN 42600 THEN big.niox_sample_amt
    END
      AS niox_feat_1,
      CASE big.niox_parameter_code
        WHEN 42601 THEN big.niox_sample_amt
    END
      AS niox_feat_2,
      CASE big.niox_parameter_code
        WHEN 42603 THEN big.niox_sample_amt
    END
      AS niox_feat_3,
      -- rh
      CASE big.rh_parameter_code
        WHEN 62103 THEN big.niox_sample_amt
    END
      AS rh_feat_1,
      CASE big.rh_parameter_code
        WHEN 62201 THEN big.niox_sample_amt
    END
      AS rh_feat_2,
      -- ws
      CASE big.ws_parameter_code
        WHEN 61103 THEN big.ws_sample_amt
    END
      AS ws_feat_1,
      CASE big.ws_parameter_code
        WHEN 61104 THEN big.ws_sample_amt
    END
      AS ws_feat_2
    FROM
      `pied-piper-project-326800.cs334.pm25_full_minus_hap` AS big
    ORDER BY
      big.state_code,
      big.county_code,
      big.date_gmt,
      big.time_gmt ) AS t
  GROUP BY
    t.state_code,
    t.county_code,
    t.date_gmt,
    t.time_gmt,
    t.latitude,
    t.longitude,
    t.pm25_sample_amt )
WHERE
  niox_feat_1_max IS NOT NULL
  AND niox_feat_2_max IS NOT NULL
  AND niox_feat_3_max IS NOT NULL
  AND rh_feat_1_max IS NOT NULL
  AND rh_feat_2_max IS NOT NULL
  AND ws_feat_1_max IS NOT NULL
  AND ws_feat_2_max IS NOT NULL
ORDER BY
  date_gmt ASC