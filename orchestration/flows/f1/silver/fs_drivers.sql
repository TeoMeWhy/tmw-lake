
WITH tb_sessions_results AS (
    SELECT *
    FROM results
    WHERE dtSession <= '{date}'
),

tb_sessions AS (

    SELECT *,
            ROW_NUMBER() OVER (PARTITION BY 1 ORDER by dtSession DESC) as rnAllSession
    FROM sessions
    WHERE descSession = 'Race'
    AND dtSession <= '{date}'
),

tb_race AS (
    SELECT t1.idDriver,
           COALESCE( CASE
                           WHEN t1.nrGridPosition == 0 THEN t1.nrPosition
                           ELSE t1.nrGridPosition
                     END,
                     t1.nrPosition) AS nrGridPosition,
           t1.nrPosition,
           t1.nrPoints,
           COALESCE( CASE
                           WHEN t1.nrGridPosition == 0 THEN t1.nrPosition
                           ELSE t1.nrGridPosition
                     END,
                     t1.nrPosition) - t1.nrPosition AS nrPositionLift,
           CASE WHEN t1.nrGridPosition = 1 THEN 1 ELSE 0 END AS flPolePosition,
           CASE WHEN t1.nrGridPosition = 1 AND t1.nrPosition = 1 THEN 1 ELSE 0 END AS flPoleWin,
           t1.dtSession,
           t1.descSessionLocation,
           (t2.qtdDrivers - t1.nrPosition + 1) / t2.qtdDrivers  AS pctPosition,
           ROW_NUMBER() OVER (PARTITION BY t1.idDriver ORDER BY t1.dtSession DESC) AS nrSessionDesc,
           ROW_NUMBER() OVER (PARTITION BY t1.idDriver ORDER BY t1.dtSession ASC) AS nrSessionAsc,
           t2.rnAllSession
           
    FROM tb_sessions_results AS t1

    LEFT JOIN tb_sessions As t2
    ON t1.idSession = t2.idSession

    WHERE t1.descSession = 'Race'
),

tb_groupbed_life AS (

    SELECT idDriver,
           '{date}' AS dtRef,
           max(dtSession) AS dtLastSeen,
           min(dtSession) AS dtFirstSeen,
           min(rnAllSession) - 1 AS qtLastSeenRace,
           avg(nrGridPosition) AS avgGridPosition,
           percentile(nrGridPosition, 0.5) AS medianGridPosition,
           avg(nrPosition) AS avgPosition,
           percentile(nrPosition, 0.5) AS medianPosition,
           sum(nrPositionLift) AS totalPositionLift,
           avg(nrPositionLift) AS avgPositionLift,
           percentile(nrPositionLift, 0.5) AS medianPositionLift,
           avg(flPolePosition) AS pctPolePositition,
           avg(flPoleWin) AS pctPolewin,
           sum(flPolePosition) AS qtdPolePositition,
           sum(flPoleWin) AS qtdPolewin
    
    FROM tb_race
    GROUP BY ALL
),

tb_groupbed_races_last_10 AS (

    SELECT idDriver,
           avg(nrGridPosition) AS avgGridPositionRaceLast10,
           percentile(nrGridPosition, 0.5) AS medianGridPositionRaceLast10,
           avg(nrPosition) AS avgPositionRaceLast10,
           percentile(nrPosition, 0.5) AS medianPositionRaceLast10,
           sum(nrPositionLift) AS totalPositionLiftRaceLast10,
           avg(nrPositionLift) AS avgPositionLiftRaceLast10,
           percentile(nrPositionLift, 0.5) AS medianPositionLiftRaceLast10,
           avg(flPolePosition) AS pctPolePosititionRaceLast10,
           avg(flPoleWin) AS pctPolewinRaceLast10
    
    FROM tb_race
    WHERE nrSessionDesc <= 10
    GROUP BY ALL
),

tb_groupbed_races_last_20 AS (

    SELECT idDriver,
           avg(nrGridPosition) AS avgGridPositionRaceLast20,
           percentile(nrGridPosition, 0.5) AS medianGridPositionRaceLast20,
           avg(nrPosition) AS avgPositionRaceLast20,
           percentile(nrPosition, 0.5) AS medianPositionRaceLast20,
           sum(nrPositionLift) AS totalPositionLiftRaceLast20,
           avg(nrPositionLift) AS avgPositionLiftRaceLast20,
           percentile(nrPositionLift, 0.5) AS medianPositionLiftRaceLast20,
           avg(flPolePosition) AS pctPolePosititionRaceLast20,
           avg(flPoleWin) AS pctPolewinRaceLast20
    
    FROM tb_race
    WHERE nrSessionDesc <= 20
    GROUP BY ALL
),

tb_groupbed_races_last_40 AS (

    SELECT idDriver,
           avg(nrGridPosition) AS avgGridPositionRaceLast40,
           percentile(nrGridPosition, 0.5) AS medianGridPositionRaceLast40,
           avg(nrPosition) AS avgPositionRaceLast40,
           percentile(nrPosition, 0.5) AS medianPositionRaceLast40,
           sum(nrPositionLift) AS totalPositionLiftRaceLast40,
           avg(nrPositionLift) AS avgPositionLiftRaceLast40,
           percentile(nrPositionLift, 0.5) AS medianPositionLiftRaceLast40,
           avg(flPolePosition) AS pctPolePosititionRaceLast40,
           avg(flPoleWin) AS pctPolewinRaceLast40
    
    FROM tb_race
    WHERE nrSessionDesc <= 40
    GROUP BY ALL
),

tb_groupbed_races_first_05 AS (

    SELECT idDriver,
           avg(nrGridPosition) AS avgGridPositionRaceFirst05,
           percentile(nrGridPosition, 0.5) AS medianGridPositionRaceFirst05,
           avg(nrPosition) AS avgPositionRaceFirst05,
           percentile(nrPosition, 0.5) AS medianPositionRaceFirst05,
           sum(nrPositionLift) AS totalPositionLiftRaceFirst05,
           avg(nrPositionLift) AS avgPositionLiftRaceFirst05,
           percentile(nrPositionLift, 0.5) AS medianPositionLiftRaceFirst05,
           avg(flPolePosition) AS pctPolePosititionRaceFirst05,
           avg(flPoleWin) AS pctPolewinRaceFirst05
    
    FROM tb_race
    WHERE nrSessionAsc <= 5
    GROUP BY ALL
),

tb_groupbed_races_first_10 AS (

    SELECT idDriver,
           avg(nrGridPosition) AS avgGridPositionRaceFirst10,
           percentile(nrGridPosition, 0.5) AS medianGridPositionRaceFirst10,
           avg(nrPosition) AS avgPositionRaceFirst10,
           percentile(nrPosition, 0.5) AS medianPositionRaceFirst10,
           sum(nrPositionLift) AS totalPositionLiftRaceFirst10,
           avg(nrPositionLift) AS avgPositionLiftRaceFirst10,
           percentile(nrPositionLift, 0.5) AS medianPositionLiftRaceFirst10,
           avg(flPolePosition) AS pctPolePosititionRaceFirst10,
           avg(flPoleWin) AS pctPolewinRaceFirst10
    
    FROM tb_race
    WHERE nrSessionAsc <= 10
    GROUP BY ALL
),

tb_final AS (

    SELECT t1.*,
            t2.avgGridPositionRaceFirst05,
            t2.medianGridPositionRaceFirst05,
            t2.avgPositionRaceFirst05,
            t2.medianPositionRaceFirst05,
            t2.totalPositionLiftRaceFirst05,
            t2.avgPositionLiftRaceFirst05,
            t2.medianPositionLiftRaceFirst05,
            t2.pctPolePosititionRaceFirst05,
            t2.pctPolewinRaceFirst05,
            
            t3.avgGridPositionRaceFirst10,
            t3.medianGridPositionRaceFirst10,
            t3.avgPositionRaceFirst10,
            t3.medianPositionRaceFirst10,
            t3.totalPositionLiftRaceFirst10,
            t3.avgPositionLiftRaceFirst10,
            t3.medianPositionLiftRaceFirst10,
            t3.pctPolePosititionRaceFirst10,
            t3.pctPolewinRaceFirst10,
    
            t4.avgGridPositionRaceLast10,
            t4.medianGridPositionRaceLast10,
            t4.avgPositionRaceLast10,
            t4.medianPositionRaceLast10,
            t4.totalPositionLiftRaceLast10,
            t4.avgPositionLiftRaceLast10,
            t4.medianPositionLiftRaceLast10,
            t4.pctPolePosititionRaceLast10,
            t4.pctPolewinRaceLast10,
    
            t5.avgGridPositionRaceLast20,
            t5.medianGridPositionRaceLast20,
            t5.avgPositionRaceLast20,
            t5.medianPositionRaceLast20,
            t5.totalPositionLiftRaceLast20,
            t5.avgPositionLiftRaceLast20,
            t5.medianPositionLiftRaceLast20,
            t5.pctPolePosititionRaceLast20,
            t5.pctPolewinRaceLast20,
    
            t6.avgGridPositionRaceLast40,
            t6.medianGridPositionRaceLast40,
            t6.avgPositionRaceLast40,
            t6.medianPositionRaceLast40,
            t6.totalPositionLiftRaceLast40,
            t6.avgPositionLiftRaceLast40,
            t6.medianPositionLiftRaceLast40,
            t6.pctPolePosititionRaceLast40,
            t6.pctPolewinRaceLast40
    
    FROM tb_groupbed_life AS t1
    
    LEFT JOIN tb_groupbed_races_first_05 AS t2
    ON t1.idDriver = t2.idDriver
    
    LEFT JOIN tb_groupbed_races_first_10 AS t3
    ON t1.idDriver = t3.idDriver
    
    LEFT JOIN tb_groupbed_races_last_10 AS t4
    ON t1.idDriver = t4.idDriver
    
    LEFT JOIN tb_groupbed_races_last_20 AS t5
    ON t1.idDriver = t5.idDriver
    
    LEFT JOIN tb_groupbed_races_last_40 AS t6
    ON t1.idDriver = t6.idDriver
)

SELECT * FROM tb_final