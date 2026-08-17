WITH tb_transacao AS (

    SELECT *,
           date(created_at) AS dtDia,
           HOUR(created_at) AS dtHora
    
    FROM bronze_points_transactions
    WHERE created_at < '{date}'

),

tb_agg_transacao AS (

    SELECT t1.id_customer,

            max(DATE_DIFF(day, t1.created_at, '{date}')) AS idadeDias,

            count(DISTINCT dtDia) AS qtdeAtivacaoVida,
            count(DISTINCT CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 7 THEN t1.dtDia END) AS qtdeAtivacaoD7,
            count(DISTINCT CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 14 THEN t1.dtDia END) AS qtdeAtivacaoD14,
            count(DISTINCT CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 28 THEN t1.dtDia END) AS qtdeAtivacaoD28,
            count(DISTINCT CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 56 THEN t1.dtDia END) AS qtdeAtivacaoD56,

            count(DISTINCT t1.uuid) AS qtdeTransacaoVida,
            count(DISTINCT CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 7 THEN t1.uuid END) AS qtdeTransacaoD7,
            count(DISTINCT CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 14 THEN t1.uuid END) AS qtdeTransacaoD14,
            count(DISTINCT CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 28 THEN t1.uuid END) AS qtdeTransacaoD28,
            count(DISTINCT CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 56 THEN t1.uuid END) AS qtdeTransacaoD56,

            sum(t1.vl_points) AS saldoVida,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 7 THEN t1.vl_points ELSE 0 END) AS saldoD7,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 14 THEN t1.vl_points ELSE 0 END) AS saldoD14,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 28 THEN t1.vl_points ELSE 0 END) AS saldoD28,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 56 THEN t1.vl_points ELSE 0 END) AS saldoD56,

            sum(CASE WHEN t1.vl_points > 0 THEN t1.vl_points ELSE 0 END ) AS qtdePontosPosVida,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 7 AND t1.vl_points > 0 THEN t1.vl_points ELSE 0 END) AS qtdePontosPosD7,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 14 AND t1.vl_points > 0 THEN t1.vl_points ELSE 0 END) AS qtdePontosPosD14,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 28 AND t1.vl_points > 0 THEN t1.vl_points ELSE 0 END) AS qtdePontosPosD28,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 56 AND t1.vl_points > 0 THEN t1.vl_points ELSE 0 END) AS qtdePontosPosD56,

            sum(CASE WHEN t1.vl_points < 0 THEN t1.vl_points ELSE 0 END ) AS qtdePontosNegVida,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 7 AND t1.vl_points < 0 THEN t1.vl_points ELSE 0 END) AS qtdePontosNegD7,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 14 AND t1.vl_points < 0 THEN t1.vl_points ELSE 0 END) AS qtdePontosNegD14,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 28 AND t1.vl_points < 0 THEN t1.vl_points ELSE 0 END) AS qtdePontosNegD28,
            sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 56 AND t1.vl_points < 0 THEN t1.vl_points ELSE 0 END) AS qtdePontosNegD56,

            count(CASE WHEN t1.dtHora BETWEEN 10 AND 14 THEN t1.uuid END) AS qtdeTransacaoManha,
            count(CASE WHEN t1.dtHora BETWEEN 15 AND 21 THEN t1.uuid END) AS qtdeTransacaoTarde,
            count(CASE WHEN t1.dtHora > 21 OR t1.dtHora < 10 THEN t1.uuid END) AS qtdeTransacaoNoite,

            1. * count(CASE WHEN t1.dtHora BETWEEN 10 AND 14 THEN t1.uuid END) / count(t1.uuid) AS pctTransacaoManha,
            1. * count(CASE WHEN t1.dtHora BETWEEN 15 AND 21 THEN t1.uuid END) / count(t1.uuid) AS pctTransacaoTarde,
            1. * count(CASE WHEN t1.dtHora > 21 OR t1.dtHora < 10 THEN t1.uuid END) / count(t1.uuid) AS pctTransacaoNoite

    FROM tb_transacao AS t1
    GROUP BY t1.id_customer

),
          
tb_agg_calc AS (

    SELECT 
        t1.*,
        COALESCE(1. * t1.qtdeTransacaoVida / t1.qtdeAtivacaoVida,0) AS QtdeTransacaoDiaVida,
        COALESCE(1. * t1.qtdeTransacaoD7 / t1.qtdeAtivacaoD7,0) AS QtdeTransacaoDiaD7,
        COALESCE(1. * t1.qtdeTransacaoD14 / t1.qtdeAtivacaoD14,0) AS QtdeTransacaoDiaD14,
        COALESCE(1. * t1.qtdeTransacaoD28 / t1.qtdeAtivacaoD28,0) AS QtdeTransacaoDiaD28,
        COALESCE(1. * t1.qtdeTransacaoD56 / t1.qtdeAtivacaoD56,0) AS QtdeTransacaoDiaD56,
        COALESCE(1. * t1.qtdeAtivacaoD28 / 28, 0) AS pctAtivacaoMAU
    FROM tb_agg_transacao AS t1

),

tb_horas_dia AS (

    SELECT t1.id_customer,
           t1.dtDia,
           (max(unix_timestamp(t1.created_at)) - min(unix_timestamp(t1.created_at)))/(60*60) AS duracao

    FROM tb_transacao AS t1
    GROUP BY t1.id_customer, t1.dtDia

),

tb_hora_cliente AS (

    SELECT t1.id_customer,
           sum(t1.duracao) AS qtdeHorasVida,
           sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 7 THEN t1.duracao ELSE 0 END) AS qtdeHorasD7,
           sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 14 THEN t1.duracao ELSE 0 END) AS qtdeHorasD14,
           sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 28 THEN t1.duracao ELSE 0 END) AS qtdeHorasD28,
           sum(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 56 THEN t1.duracao ELSE 0 END) AS qtdeHorasD56

    FROM tb_horas_dia AS t1
    GROUP BY t1.id_customer
),

tb_lag_dia AS (

    SELECT t1.id_customer,
           t1.dtDia,
           LAG(t1.dtDia) OVER (PARTITION BY t1.id_customer order by t1.dtDia) AS lagDia
    FROM tb_horas_dia AS t1

),

tb_intervalo_dias AS (

    SELECT t1.id_customer,
           avg(DATE_DIFF(day, t1.lagDia, t1.dtDia)) AS avgIntervaloDiasVida,
           avg(CASE WHEN DATE_DIFF(day, t1.dtDia, date('{date}')) <= 28 THEN DATE_DIFF(day, t1.lagDia, t1.dtDia) END) AS avgIntervaloDiasD28

    FROM tb_lag_dia AS t1
    GROUP BY t1.id_customer

),
          
tb_share_produtos AS (

    SELECT 
        t1.id_customer,
        1. * COUNT(CASE WHEN t3.desc_product = 'ChatMessage' THEN t1.uuid END) / count(t1.uuid) AS qteChatMessage,
        1. * COUNT(CASE WHEN t3.desc_product = 'Airflow Lover' THEN t1.uuid END) / count(t1.uuid) AS qteAirflowLover,
        1. * COUNT(CASE WHEN t3.desc_product = 'R Lover' THEN t1.uuid END) / count(t1.uuid) AS qteRLover,
        1. * COUNT(CASE WHEN t3.desc_product = 'Resgatar Ponei' THEN t1.uuid END) / count(t1.uuid) AS qteResgatarPonei,
        1. * COUNT(CASE WHEN t3.desc_product = 'Lista de presença' THEN t1.uuid END) / count(t1.uuid) AS qteListadepresenca,
        1. * COUNT(CASE WHEN t3.desc_product = 'Presença Streak' THEN t1.uuid END) / count(t1.uuid) AS qtePresencaStreak,
        1. * COUNT(CASE WHEN t3.desc_product = 'Troca de Pontos StreamElements' THEN t1.uuid END) / count(t1.uuid) AS qteTrocaStreamElements,
        1. * COUNT(CASE WHEN t3.desc_product = 'Reembolso: Troca de Pontos StreamElements' THEN t1.uuid END) / count(t1.uuid) AS qteReembolsoStreamElements,
        1. * COUNT(CASE WHEN desc_product_category = 'rpg' THEN t1.uuid END) / count(t1.uuid) AS qtdeRPG,
        1. * COUNT(CASE WHEN desc_product_category = 'churn_model' THEN t1.uuid END) / count(t1.uuid) AS qtdeChurnModel

    FROM tb_transacao AS t1

    LEFT JOIN bronze_points_transaction_products AS t2
    ON t1.uuid = t2.id_transaction

    LEFT JOIN bronze_points_products AS t3
    ON t2.cod_product = t3.uuid

    GROUP BY t1.id_customer

),

tb_join AS (

    SELECT t1.*,
        t2.qtdeHorasVida,
        t2.qtdeHorasD7,
        t2.qtdeHorasD14,
        t2.qtdeHorasD28,
        t2.qtdeHorasD56,
        t3.avgIntervaloDiasVida,
        t3.avgIntervaloDiasD28,
        t4.qteChatMessage,
        t4.qteAirflowLover,
        t4.qteRLover,
        t4.qteResgatarPonei,
        t4.qteListadepresenca,
        t4.qtePresencaStreak,
        t4.qteTrocaStreamElements,
        t4.qteReembolsoStreamElements,
        t4.qtdeRPG,
        t4.qtdeChurnModel

    FROM tb_agg_calc AS t1

    LEFT JOIN tb_hora_cliente AS t2
    ON t1.id_customer = t2.id_customer

    LEFT JOIN tb_intervalo_dias AS t3
    ON t1.id_customer = t3.id_customer

    LEFT JOIN tb_share_produtos AS t4
    ON t1.id_customer = t4.id_customer

)
          
SELECT date('{date}') AS dtRef,
       *

FROM tb_join