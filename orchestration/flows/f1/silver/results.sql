WITH renamed AS (

    SELECT
            date(date) || '-' || location || '-' || event_name AS idSession,
            Abbreviation AS descDriverAbreviation,
            DriverId As idDriver,
            DriverNumber AS nrDriver,
            FirstName AS descDriverName,
            FullName AS descDriverFullName,
            int(ClassifiedPosition) AS nrClassifiedPosition,
            int(GridPosition) AS nrGridPosition,
            int(Position) AS nrPosition,
            int(Points) AS nrPoints,
            TeamId AS idTeam,
            TeamName AS descTeamName,
            Time AS descTimeDuration,
            country AS descCountryName,
            location AS descSessionLocation,
            date(date) AS dtSession,
            event_name AS descSession
            
    FROM f1_results
)

SELECT * FROM renamed