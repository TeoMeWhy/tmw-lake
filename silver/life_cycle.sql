WITH
tb_daily AS (
	SELECT DISTINCT id_customer AS IdCliente,
			DATE(created_at) AS dtDia
	FROM bronze_points_transactions
	WHERE created_at < DATE('{date}')
),

tb_idade AS (
	SELECT
		IdCliente,
		MAX(DATE_DIFF(day, dtDia, DATE('{date}'))) AS qtdeDiasPrimTransacao,
		MIN(DATE_DIFF(day, dtDia, DATE('{date}'))) AS qtdeDiasUltTransacao
	FROM tb_daily
	GROUP BY IdCliente
),

tb_rn AS (
	SELECT *,
		   row_number() OVER (PARTITION BY IdCliente ORDER BY dtDia DESC) AS rnDia
	FROM tb_daily
),

tb_penultima_ativacao AS (
	SELECT
		*,
		DATE_DIFF(day, dtDia, DATE('{date}')) AS qtdeDiasPenultimaTransacao
	FROM tb_rn
	WHERE rnDia = 2
),

tb_life_cycle AS (
	SELECT
		t1.*,
		t2.qtdeDiasPenultimaTransacao,
		CASE
			WHEN qtdeDiasPrimTransacao <= 7 THEN '01-CURIOSO'
			WHEN qtdeDiasUltTransacao <= 7
			AND qtdeDiasPenultimaTransacao - qtdeDiasUltTransacao <= 14 THEN '02-FIEL'
			WHEN qtdeDiasUltTransacao BETWEEN 8 AND 14  THEN '03-TURISTA'
			WHEN qtdeDiasUltTransacao BETWEEN 15 AND 28  THEN '04-DESENCANTADA'
			WHEN qtdeDiasUltTransacao > 28 THEN '05-ZUMBI'
			WHEN qtdeDiasUltTransacao <= 7
			AND qtdeDiasPenultimaTransacao - qtdeDiasUltTransacao BETWEEN 15 AND 27  THEN '02-RECONQUISTADO'
			WHEN qtdeDiasUltTransacao <= 7
			AND qtdeDiasPenultimaTransacao - qtdeDiasUltTransacao > 27 THEN '02-REBORN'
		END AS descLifeCycle
	FROM tb_idade AS t1
	LEFT JOIN tb_penultima_ativacao AS t2 ON t1.idCliente = t2.idCliente
),

tb_freq_valor AS (
	SELECT
		id_customer AS idCliente,
		count(DISTINCT DATE(created_at)) AS qtdeFrequencia,
		sum(CASE WHEN vl_points > 0 THEN vl_points
				 ELSE 0
			END) AS qtdePontosPos
	FROM bronze_points_transactions
	WHERE created_at < DATE('{date}')
	AND DATE_DIFF (day, created_at, DATE('{date}')) < 28
	
	GROUP BY id_customer
	ORDER BY qtdeFrequencia DESC
),

tb_cluster AS (
	SELECT
		*,
		CASE
			WHEN qtdeFrequencia <= 10
			AND qtdePontosPos >= 1500 THEN '12-HYPER'
			WHEN qtdeFrequencia > 10
			AND qtdePontosPos >= 1500 THEN '22-EFICIENTE'
			WHEN qtdeFrequencia <= 10
			AND qtdePontosPos >= 750 THEN '11-INDECISO'
			WHEN qtdeFrequencia > 10
			AND qtdePontosPos >= 750 THEN '21-ESFORÇADO'
			WHEN qtdeFrequencia < 5 THEN '00-LURKER'
			WHEN qtdeFrequencia <= 10 THEN '01-PREGUIÇOSO'
			WHEN qtdeFrequencia > 10 THEN '20-POTENCIAL'
		END AS cluster
	FROM tb_freq_valor
),

tb_final AS (
	SELECT
		DATE('{date}') AS dtRef,
		t1.*,
		t2.qtdeFrequencia,
		t2.qtdePontosPos,
		t2.cluster
	
	FROM tb_life_cycle AS t1
	
	LEFT JOIN tb_cluster AS t2
	ON t1.IdCliente = t2.IdCliente
)

SELECT *
FROM tb_final