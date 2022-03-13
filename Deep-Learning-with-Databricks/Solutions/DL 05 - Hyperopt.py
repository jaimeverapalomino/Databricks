# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Hyperopt
# MAGIC 
# MAGIC The <a href="https://github.com/hyperopt/hyperopt" target="_blank">Hyperopt library</a> allows for parallel hyperparameter tuning using either random search or Tree of Parzen Estimators (TPE). With MLflow, we can record the hyperparameters and corresponding metrics for each hyperparameter combination. You can read more on <a href="https://github.com/hyperopt/hyperopt/blob/master/docs/templates/scaleout/spark.md" target="_blank">SparkTrials w/ Hyperopt</a>.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:
# MAGIC  - Use Hyperopt to train and optimize a feed-forward neural net

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

from sklearn.datasets import fetch_california_housing
from sklearn.model_selection import train_test_split

cal_housing = fetch_california_housing()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(cal_housing.data,
                                                    cal_housing.target,
                                                    test_size=0.2,
                                                    random_state=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Standardization
# MAGIC Let's do feature-wise standardization.

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Keras Model
# MAGIC 
# MAGIC We will define our NN in Keras and use the hyperparameters given by HyperOpt.

# COMMAND ----------

import tensorflow as tf
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential
tf.random.set_seed(42)

def create_model(hpo):
    model = Sequential()
    model.add(Dense(int(hpo["dense_l1"]), input_dim=8, activation="relu"))
    model.add(Dense(int(hpo["dense_l2"]), activation="relu"))
    model.add(Dense(1, activation="linear"))
    return model

# COMMAND ----------

from hyperopt import fmin, hp, tpe, SparkTrials

def run_nn(hpo):
    model = create_model(hpo)

    # Select Optimizer
    optimizer_call = getattr(tf.keras.optimizers, hpo["optimizer"])
    optimizer = optimizer_call(learning_rate=hpo["learning_rate"])

    # Compile model
    model.compile(loss="mse",
                  optimizer=optimizer,
                  metrics=["mse"])

    history = model.fit(X_train, y_train, validation_split=.2, batch_size=64, epochs=10, verbose=2)

    # Evaluate our model
    obj_metric = history.history["val_loss"][-1]
    return obj_metric

# COMMAND ----------

# MAGIC %md ### Setup hyperparameter space and training
# MAGIC 
# MAGIC We need to create a search space for HyperOpt and set up SparkTrials to allow HyperOpt to run in parallel using Spark worker nodes. MLflow will automatically track the results of HyperOpt's tuning trials.

# COMMAND ----------

space = {
    "dense_l1": hp.quniform("dense_l1", 10, 30, 1),
    "dense_l2": hp.quniform("dense_l2", 10, 30, 1),
    "learning_rate": hp.loguniform("learning_rate", -5, 0),
    "optimizer": hp.choice("optimizer", ["Adadelta", "Adam"])
 }

spark_trials = SparkTrials(parallelism=4)

best_hyperparam = fmin(fn=run_nn, 
                       space=space, 
                       algo=tpe.suggest, 
                       max_evals=16, 
                       trials=spark_trials,
                       rstate=np.random.default_rng(42))

best_hyperparam

# COMMAND ----------

# MAGIC %md
# MAGIC To view the MLflow experiment associated with the notebook, click the Runs icon in the notebook context bar on the upper right. There, you can view all runs. You can also bring up the full MLflow UI by clicking the button on the upper right that reads View Experiment UI when you hover over it.
# MAGIC 
# MAGIC To understand the effect of tuning a hyperparameter:
# MAGIC 
# MAGIC 0. Select the resulting runs and click Compare.
# MAGIC 0. In the Scatter Plot, select a hyperparameter for the X-axis and loss for the Y-axis.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
