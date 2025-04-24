WITH driver_season AS (
    SELECT DISTINCT idDriver, YEAR(dtsession) AS yearSeason
    FROM results
),

tb_dtref_races (
    SELECT distinct dtSession,
           rank(dtSession) OVER (PARTITION BY year(dtSession) ORDER BY dtSession) AS rnSessionYear
    FROM results
    WHERE descSession = 'Race'
    AND dtSession > '2000-01-01'
),

tb_abt AS (

    SELECT t1.*,
           CASE WHEN t2.yearSeason IS NULL THEN 1 ELSE 0 END AS flChurn

    FROM fs_drivers AS t1

    LEFT JOIN driver_season AS t2
    ON t1.idDriver = t2.idDriver
    AND YEAR(t1.dtRef) = yearSeason - 1

    INNER JOIN tb_dtref_races AS t3
    ON t1.dtRef = t3.dtSession

    WHERE year(t1.dtRef) < year(now())
    AND t1.qtLastSeenRace <= 2
    AND dtFirstSeen >= '1991-01-01'
    AND NOT(
        t1.qtLastSeenRace = 1 AND t3.rnSessionYear = 1
    )
    

    ORDER BY dtRef DESC, idDriver
)

SELECT * FROM tb_abt