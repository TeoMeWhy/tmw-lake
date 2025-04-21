import streamlit as st

import matplotlib.pyplot as plt
import mplcyberpunk
plt.style.use("cyberpunk")

from database import connect_spark, get_predictions


SPARK = connect_spark()
data = get_predictions(SPARK)

st.markdown("# F1 Churn Score")

cols = st.columns(3)
with cols[0]:
    model = st.selectbox("Modelo de churn", data["nrModelVersion"].unique(), index=0)
    text = "Confira os detalhes do modelo aqui"
    model_mlflow = f"http://localhost:5000/#/models/f1_churn/versions/{model}"
    st.link_button("Detalhes do modelo", model_mlflow, help=text)

with cols[1]:
    top = st.toggle("Top N", value=True)

with cols[2]:
    if top:
        sorted = data[data["dtRef"]==data["dtRef"].max()].sort_values("pctProbaChurn", ascending=False)
        n = st.slider("Quantidade de Pilotos (N)", 1, sorted["idDriver"].nunique(), 5)
        reverse = st.toggle("Reverse", value=False)
        if reverse:
            drivers = sorted.tail(n)['idDriver'].unique()
        else:
            drivers = sorted.head(n)['idDriver'].unique()
    else:
        drivers = st.multiselect("Piloto", data["idDriver"].unique(), default=data["idDriver"].unique())

filters = data["nrModelVersion"] == model
filters *= data["idDriver"].isin(drivers)

data_show = data[filters].rename(columns={"dtRef": "Data",
                                          "pctProbaChurn": "Probabilidade Churn",
                                          "idDriver": "Piloto",
                                          })


plot = st.line_chart(data_show, x="Data", y="Probabilidade Churn", color="Piloto")

st.dataframe(data_show)