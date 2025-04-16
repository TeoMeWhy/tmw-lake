WITH sessions AS (

    SELECT 
        idSession,
        year(dtSession) AS nrSeasonYear,
        dtSession,
        descCountryName,
        descSessionLocation,
        descSession,
        count(distinct idDriver) AS qtdDrivers,
        count(distinct CASE WHEN nrGridPosition >= 1 THEN idDriver END) AS qtdDriversGrid

    FROM results

    GROUP BY ALL
    ORDER BY dtSession
)

SELECT *
FROM sessions