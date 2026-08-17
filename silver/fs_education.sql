WITH tb_usuario_cursos AS (
    SELECT userID,
           courseSlug,
           COUNT(epSlug) AS qtdeEps

    FROM bronze_education_courses_ep_complete
    WHERE createdAt < DATE('{date}')
    GROUP BY userID, courseSlug
),

tb_cursos_total_eps AS (

    SELECT slug AS courseSlug,
           COUNT(ep) as qtdeTotalEps
    FROM bronze_education_courses_eps
    GROUP BY slug

),

tb_pct_cursos AS (

    SELECT t1.userID,
            t1.courseSlug,
            1. * t1.qtdeEps / t2.qtdeTotalEps AS pctCursoCompleto

    FROM tb_usuario_cursos AS t1

    LEFT JOIN tb_cursos_total_eps AS t2
    ON t1.courseSlug = t2.courseSlug

),

tb_pct_cursos_pivot AS (

    SELECT userID,
        SUM(CASE WHEN pctCursoCompleto = 1 then 1 else 0 end) AS qtdeCursosCompletos,
        SUM(CASE WHEN pctCursoCompleto > 0 and pctCursoCompleto < 1 then 1 else 0 end) AS qtdeCursosIncompletos,
        SUM(CASE WHEN courseSlug = 'carreira' THEN pctCursoCompleto ELSE 0 END) AS carreira,
        SUM(CASE WHEN courseSlug = 'coleta-dados-2024' THEN pctCursoCompleto ELSE 0 END) AS coletaDados2024,
        SUM(CASE WHEN courseSlug = 'ds-databricks-2024' THEN pctCursoCompleto ELSE 0 END) AS dsDatabricks2024,
        SUM(CASE WHEN courseSlug = 'ds-pontos-2024' THEN pctCursoCompleto ELSE 0 END) AS dsPontos2024,
        SUM(CASE WHEN courseSlug = 'estatistica-2024' THEN pctCursoCompleto ELSE 0 END) AS estatistica2024,
        SUM(CASE WHEN courseSlug = 'estatistica-2025' THEN pctCursoCompleto ELSE 0 END) AS estatistica2025,
        SUM(CASE WHEN courseSlug = 'github-2024' THEN pctCursoCompleto ELSE 0 END) AS github2024,
        SUM(CASE WHEN courseSlug = 'github-2025' THEN pctCursoCompleto ELSE 0 END) AS github2025,
        SUM(CASE WHEN courseSlug = 'ia-canal-2025' THEN pctCursoCompleto ELSE 0 END) AS iaCanal2025,
        SUM(CASE WHEN courseSlug = 'lago-mago-2024' THEN pctCursoCompleto ELSE 0 END) AS lagoMago2024,
        SUM(CASE WHEN courseSlug = 'machine-learning-2025' THEN pctCursoCompleto ELSE 0 END) AS machineLearning2025,
        SUM(CASE WHEN courseSlug = 'matchmaking-trampar-de-casa-2024' THEN pctCursoCompleto ELSE 0 END) AS matchmakingTramparDeCasa2024,
        SUM(CASE WHEN courseSlug = 'ml-2024' THEN pctCursoCompleto ELSE 0 END) AS ml2024,
        SUM(CASE WHEN courseSlug = 'mlflow-2025' THEN pctCursoCompleto ELSE 0 END) AS mlflow2025,
        SUM(CASE WHEN courseSlug = 'pandas-2024' THEN pctCursoCompleto ELSE 0 END) AS pandas2024,
        SUM(CASE WHEN courseSlug = 'pandas-2025' THEN pctCursoCompleto ELSE 0 END) AS pandas2025,
        SUM(CASE WHEN courseSlug = 'python-2024' THEN pctCursoCompleto ELSE 0 END) AS python2024,
        SUM(CASE WHEN courseSlug = 'python-2025' THEN pctCursoCompleto ELSE 0 END) AS python2025,
        SUM(CASE WHEN courseSlug = 'sql-2020' THEN pctCursoCompleto ELSE 0 END) AS sql2020,
        SUM(CASE WHEN courseSlug = 'sql-2025' THEN pctCursoCompleto ELSE 0 END) AS sql2025,
        SUM(CASE WHEN courseSlug = 'streamlit-2025' THEN pctCursoCompleto ELSE 0 END) AS streamlit2025,
        SUM(CASE WHEN courseSlug = 'trampar-lakehouse-2024' THEN pctCursoCompleto ELSE 0 END) AS tramparLakehouse2024,
        SUM(CASE WHEN courseSlug = 'tse-analytics-2024' THEN pctCursoCompleto ELSE 0 END) AS tseAnalytics2024

    FROM tb_pct_cursos

    GROUP BY userID  

),

tb_atividade AS (

    SELECT 
        userID,
        max(createdAt) as dtCriacao

    FROM bronze_education_user_rewards
    WHERE createdAt < DATE('{date}')
    GROUP BY userID

    UNION ALL

    SELECT 
        userID,
        max(createdAt) AS dtCriacao

    FROM bronze_education_user_skills
    WHERE createdAt < DATE('{date}')
    GROUP BY userID

    UNION ALL

    SELECT
        userID,
        max(createdAt) AS dtCriacao

    FROM bronze_education_courses_ep_complete
    WHERE createdAt < DATE('{date}')
    GROUP BY userID

),

tb_ultima_atividade AS (

    SELECT userID,
            MIN( DATE_DIFF(day, DATE('{date}'), dtCriacao)) AS qtdDiasUltiAtividade
    FROM tb_atividade
    GROUP BY userID

),

tb_join AS (

    SELECT t3.tmwID AS idCliente,
        t1.qtdeCursosCompletos,
        t1.qtdeCursosIncompletos,
        t1.carreira,
        t1.coletaDados2024,
        t1.dsDatabricks2024,
        t1.dsPontos2024,
        t1.estatistica2024,
        t1.estatistica2025,
        t1.github2024,
        t1.github2025,
        t1.iaCanal2025,
        t1.lagoMago2024,
        t1.machineLearning2025,
        t1.matchmakingTramparDeCasa2024,
        t1.ml2024,
        t1.mlflow2025,
        t1.pandas2024,
        t1.pandas2025,
        t1.python2024,
        t1.python2025,
        t1.sql2020,
        t1.sql2025,
        t1.streamlit2025,
        t1.tramparLakehouse2024,
        t1.tseAnalytics2024,
        t2.qtdDiasUltiAtividade

    FROM tb_pct_cursos_pivot as t1

    LEFT JOIN tb_ultima_atividade AS t2
    ON t1.userID = t2.userID

    INNER JOIN bronze_education_tmw_users AS t3
    ON t1.userID = t3.userID

)

SELECT DATE('{date}') AS dtRef,
       *
FROM tb_join