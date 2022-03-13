# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # MLflow
# MAGIC 
# MAGIC How do you remember which network architecture and hyperparameters performed the worked best? That's where <a href="https://mlflow.org/" target="_blank">MLflow</a> comes into play!
# MAGIC 
# MAGIC <a href="https://mlflow.org/docs/latest/concepts.html" target="_blank">MLflow</a> seeks to address these three core issues:
# MAGIC 
# MAGIC * It’s difficult to keep track of experiments
# MAGIC * It’s difficult to reproduce code
# MAGIC * There’s no standard way to package and deploy models
# MAGIC 
# MAGIC In this notebook, we will show how to do experiment tracking with MLflow! We will start with logging the metrics from the models we created with the California housing dataset.
# MAGIC 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Log experiments with MLflow
# MAGIC  - View MLflow UI
# MAGIC  - Generate a UDF with MLflow and apply to a Spark DataFrame
# MAGIC 
# MAGIC MLflow is pre-installed on the Databricks Runtime for ML.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

from sklearn.datasets import fetch_california_housing
from sklearn.model_selection import train_test_split
import tensorflow as tf
tf.random.set_seed(42)

cal_housing = fetch_california_housing()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(cal_housing.data,
                                                    cal_housing.target,
                                                    test_size=0.2,
                                                    random_state=1)

print(cal_housing.DESCR)

# COMMAND ----------

# MAGIC %md
# MAGIC Build model architecture as before.

# COMMAND ----------

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense

def build_model():
    return Sequential([Dense(20, input_dim=8, activation="relu"),
                       Dense(20, activation="relu"),
                       Dense(1, activation="linear")]) # Keep the last layer as linear because this is a regression problem

# COMMAND ----------

# MAGIC %md-sandbox ### Start Using MLflow in a Notebook
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-tracking.png" style="height: 300px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md Helper method to plot our training loss using matplotlib.

# COMMAND ----------

import matplotlib.pyplot as plt

def view_model_loss(history):
    plt.clf()
    plt.plot(history.history["loss"], label="train_loss")
    if "val_loss" in history.history:
        plt.plot(history.history["val_loss"], label="val_loss")
    plt.title("Model Loss")
    plt.ylabel("Loss")
    plt.xlabel("Epoch")
    plt.legend()
    return plt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Track experiments!
# MAGIC 
# MAGIC When traking an experiment, you can use <a href="https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.set_experiment" target="_blank">**`mlflow.set_experiment()`**</a> to set an experiment, but if you do not specify an experiment, it will automatically be scoped to this notebook.
# MAGIC 
# MAGIC Additionally, when training a model you can log to MLflow using <a href="https://docs.databricks.com/applications/mlflow/databricks-autologging.html" target="_blank">autologging</a>. Autologging allows you to log metrics, parameters, and models without the need for explicit log statements.
# MAGIC 
# MAGIC There are a few ways to use autologging:
# MAGIC 
# MAGIC   1. Call **`mlflow.autolog()`** before your training code. This will enable autologging for each supported library you have installed as soon as you import it.
# MAGIC 
# MAGIC   2. Enable autologging at the workspace level from the admin console
# MAGIC 
# MAGIC   3. Use library-specific autolog calls for each library you use in your code. (e.g. **`mlflow.tensorflow.autolog()`**)
# MAGIC 
# MAGIC Here we are only using numeric features for simplicity of building the random forest.

# COMMAND ----------

import mlflow

# Note issue with **kwargs https://github.com/keras-team/keras/issues/9805
def track_experiments(run_name, model, compile_kwargs, fit_kwargs, optional_params={}):
    with mlflow.start_run(run_name=run_name) as run:
        
        # Enable autologging - need to put in the with statement to keep the run id
        mlflow.tensorflow.autolog(log_models=True)
        
        model = model()
        model.compile(**compile_kwargs)
        history = model.fit(**fit_kwargs)
            
        # Log optional params 
        mlflow.log_params(optional_params)

        plt = view_model_loss(history)
        fig = plt.gcf()
        mlflow.log_figure(fig, "train-validation-loss.png")

        return run

# COMMAND ----------

# MAGIC %md
# MAGIC Let's recall what happend when we used ADAM.

# COMMAND ----------

compile_kwargs = {
    "optimizer": "adam",
    "loss": "mse",
    "metrics": ["mse", "mae"]
}

fit_kwargs = {
    "x": X_train, 
    "y": y_train,
    "epochs": 10,
    "verbose": 2,
    "batch_size": 64
}

optional_params = {
    "standardize_data": "false"
}

run_name = "ADAM"
run = track_experiments(run_name, build_model, compile_kwargs, fit_kwargs, optional_params)

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's add some data standardization, as well as a validation dataset.

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

fit_kwargs["x"] = X_train_scaled
fit_kwargs["validation_split"] = 0.2

optional_params = {
    "standardize_data": "true"
}

run_name = "StandardizedValidation"
run = track_experiments(run_name, build_model, compile_kwargs, fit_kwargs, optional_params)

# COMMAND ----------

# MAGIC %md ### Querying Past Runs
# MAGIC 
# MAGIC You can query past runs programatically in order to use this data back in Python.  The pathway to doing this is an **`MlflowClient`** object. 

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()

client.list_experiments()

# COMMAND ----------

# MAGIC %md You can also use <a href="https://mlflow.org/docs/latest/search-syntax.html" target="_blank">search_runs</a> to find all runs for a given experiment.

# COMMAND ----------

runs_df = mlflow.search_runs(run.info.experiment_id)

display(runs_df)

# COMMAND ----------

# MAGIC %md Pull the last run and look at metrics. 

# COMMAND ----------

runs = client.search_runs(run.info.experiment_id, order_by=["attributes.start_time desc"], max_results=1)
runs[0].data.metrics

# COMMAND ----------

# MAGIC %md ## User Defined Function
# MAGIC 
# MAGIC Let's now register our Keras model as a Spark UDF to apply to rows in parallel.

# COMMAND ----------

import pandas as pd

predict = mlflow.pyfunc.spark_udf(spark, f"runs:/{runs[0].info.run_id}/model") 

X_test_df = spark.createDataFrame(pd.concat([pd.DataFrame(X_test_scaled, columns=cal_housing.feature_names), 
                                             pd.DataFrame(y_test, columns=["label"])], axis=1))

display(X_test_df.withColumn("prediction", predict(*cal_housing.feature_names)))

# COMMAND ----------

# MAGIC %md Register the Vectorized UDF **`predict`** into the SQL namespace.

# COMMAND ----------

spark.udf.register("predictUDF", predict)
X_test_df.createOrReplaceGlobalTempView("X_test_df")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT *, predictUDF(MedInc, HouseAge, AveRooms, AveBedrms, Population, AveOccup, Latitude, Longitude) AS prediction 
# MAGIC FROM global_temp.X_test_df

# COMMAND ----------

# MAGIC %md Now, go back and add MLflow to your experiments from the Wine Quality Dataset!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
