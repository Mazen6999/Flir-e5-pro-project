SELECT
    Final.Department AS Department,
    Final."asset_name" AS Asset,
    Final."Last Time" AS "Last Reading",
    'Check Asset' AS Recommendation,

    CASE
        WHEN Final."Last Reading" >= 70 THEN 'Temperature is higher than 70°C'
        WHEN Final."Delta" >= 5 THEN 'Temperature increased by more than 5°C'
        ELSE 'Normal'
    END AS Reason

FROM (
    SELECT
        r1."asset_name",
        r1.Department,
        r1."Center_Temp_C" AS "Last Reading",
        r1."Timestamp"  AS "Last Time",
        (r1."Center_Temp_C" - r2."Center_Temp_C") AS "Delta"

    FROM (
        SELECT 
            A."asset_name",
            A."section" AS Department,
            T."Center_Temp_C",
            T."Timestamp",
            ROW_NUMBER() OVER (
                PARTITION BY A."asset_name"
                ORDER BY T."Timestamp" DESC
            ) AS rn
        FROM "EG_CA_TRU_FAC_LOC01".Flir."ThermalReadings" T
        JOIN "EG_CA_TRU_FAC_LOC01".Flir."Assets_Catalog" A
          ON T."Asset_Name" = A."asset_code"
    ) r1

    JOIN (
        SELECT 
            A."asset_name",
            A."section" AS Department,
            T."Center_Temp_C",
            T."Timestamp",
            ROW_NUMBER() OVER (
                PARTITION BY A."asset_name"
                ORDER BY T."Timestamp" DESC
            ) AS rn
        FROM "EG_CA_TRU_FAC_LOC01".Flir."ThermalReadings" T
        JOIN "EG_CA_TRU_FAC_LOC01".Flir."Assets_Catalog" A
          ON T."Asset_Name" = A."asset_code"
    ) r2

    ON r1."asset_name" = r2."asset_name"
   AND r1.rn = 1
   AND r2.rn = 2

    WHERE 
        r1."Center_Temp_C" >= 70
        OR (r1."Center_Temp_C" - r2."Center_Temp_C") >= 5
) Final

ORDER BY Final.Department, Final."Last Time" DESC;
