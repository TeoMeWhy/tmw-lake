WITH driver_season AS (
    SELECT DISTINCT idDriver, YEAR(dtsession) AS yearSeason
    FROM results
),

tb_dtref_races (
    SELECT DISTINCT dtSession
    FROM results
    WHERE descSession = 'Race'
),

tb_abt AS (

    SELECT t1.*,
           CASE WHEN t2.yearSeason IS NULL THEN 1 ELSE 0 END AS flChurn

    FROM fs_drivers AS t1

    LEFT JOIN driver_season AS t2
    ON t1.idDriver = t2.idDriver
    AND YEAR(t1.dtRef) = yearSeason - 1

    WHERE year(t1.dtRef) < year(now())
    AND t1.qtLastSeenRace <= 2
    AND t1.dtRef IN (SELECT dtSession FROM tb_dtref_races)

    ORDER BY dtRef DESC, idDriver
)

SELECT * FROM tb_abt