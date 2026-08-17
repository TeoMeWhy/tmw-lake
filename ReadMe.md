# Datalake - Téo Me Why

## Sobre

Projeto de construção de uma Datalake/Lakehouse com ferramentas abertas.

## Etapas

1. Coleta de dados da fastF1 na camada Raw
2. Consolidação em Bronze com Spark + Delta
3. Modelagem de dados na camada Silver
4. Construção de Feature Store
5. Treino de Modelos de Machine Learning
6. Orquestração de Pipeline

### 1. Coleta de dados

1.1. Setup local

```bash
conda create --name lake-raw python=3.

conda activate lake-raw

pip install pandas fastf1

```

