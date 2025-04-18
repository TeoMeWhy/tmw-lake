# %%
import pandas as pd
from pyspark.sql import SparkSession

import mlflow
from feature_engine import encoding
from sklearn import ensemble
from sklearn import metrics
from sklearn import model_selection
from sklearn import pipeline

spark = (SparkSession.builder
                     .appName("Analytics")
                     .master("local[4]")
                     .getOrCreate())

# %%
df = (spark.read
           .format("delta")
           .load("s3a://analytics/f1/abt_churn")
           .toPandas())

print(df.head())

# %%

df["dtSeason"] = pd.to_datetime(df["dtRef"]).dt.year
df_drivers_season = df[["idDriver", "dtSeason"]].drop_duplicates()

# %%
target = "flChurn"
features = df.columns.tolist()[4:-2]

train, test = model_selection.train_test_split(df_drivers_season,
                                               test_size=0.2,
                                               random_state=42,
                                               )

train = train.merge(df, how="inner", on=["idDriver", "dtSeason"])
test  = test.merge(df, how="inner", on=["idDriver", "dtSeason"])

X_train = train[features]
X_test  = test[features]

y_train = train[target]
y_test  = test[target]

print("Base de treino:",X_train.shape[0],"linhas com", y_train.mean(), "pct na variável target")
print("Base de test:", X_test.shape[0], "linhas com", y_test.mean(), "pct na variável target")

# %%

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment(experiment_name="f1_churn")

with mlflow.start_run() as run:
    mlflow.sklearn.autolog()
    
    estimator = ensemble.RandomForestClassifier(n_estimators=2, max_depth=1, random_state=42)
    clf = ensemble.AdaBoostClassifier(estimator=estimator, n_estimators=1000, learning_rate=0.02)
    onehot = encoding.OneHotEncoder(variables=["idTeam"])

    model_pipeline = pipeline.Pipeline(steps=[
        ("onehot",onehot),
        ("model",clf),
    ])

    model_pipeline.fit(X_train, y_train)

    predict_train = model_pipeline.predict(X_train)
    proba_train = model_pipeline.predict_proba(X_train)[:,1]
    predict_test = model_pipeline.predict(X_test)
    proba_test = model_pipeline.predict_proba(X_test)[:,1]

    acc_train = metrics.accuracy_score(y_train, predict_train)
    auc_train = metrics.roc_auc_score(y_train, proba_train)
    acc_test = metrics.accuracy_score(y_test, predict_test)
    auc_test = metrics.roc_auc_score(y_test, proba_test)

    print("ACC Treino:", acc_train)
    print("AUC Treino:",auc_train)
    print("ACC Test:", acc_test)
    print("AUC Test:",auc_test)

    mlflow.log_metrics({
        "acc_train":acc_train,
        "auc_train":auc_train,
        "acc_test":acc_test,
        "auc_test":auc_test,
    })

# %%
run_actual = mlflow.get_run(run_id=run.info.run_id)
actual_auc = run_actual.data._metrics["auc_test"]

try:
    model = mlflow.search_registered_models(filter_string="name='f1_churn'")[0]
    model_versions = model.latest_versions
    model_versions.sort(key=lambda x: x.version)
    model = model_versions[-1]

    registered_model_run = mlflow.get_run(model.run_id)
    registered_model_auc = registered_model_run.data._metrics["auc_test"]

except IndexError as err:
    registered_model_auc = 0

if actual_auc > registered_model_auc:
    mlflow.register_model(f"runs:/{run.info.run_id}/model", "f1_churn")


# %%
columns = model_pipeline[:-1].transform(X_train.head(1)).columns
df_importance = pd.Series(clf.feature_importances_, index=columns).sort_values(ascending=False).reset_index()
df_importance.columns = ["feature", "importance"]
df_importance["importance_acum"] = df_importance["importance"].cumsum()
print(df_importance)
