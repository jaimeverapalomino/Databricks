# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Hyperopt Lab
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Use Hyperopt to find the best hyperparameters for the wine quality dataset!

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

import pandas as pd
from sklearn.datasets import load_wine

# Import Dataset
wine_quality = load_wine(as_frame=True)

# COMMAND ----------

from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(wine_quality.data,
                                                    wine_quality.target,
                                                    test_size=0.2,
                                                    random_state=1)

# Scale features
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
    model.add(Dense(int(hpo["dense_l1"]), input_dim=13, activation="relu")) # You can change the activation functions too!
    model.add(Dense(int(hpo["dense_l2"]), activation="relu"))
    model.add(Dense(1, activation="linear"))
    return model

# COMMAND ----------

from hyperopt import fmin, hp, tpe, SparkTrials

def run_nn(hpo):
    model = create_model(hpo)

    # Select Optimizer
    optimizer_call = getattr(tf.keras.optimizers, hpo["optimizer"])
    optimizer = optimizer_call(hpo["learning_rate"])

    # Compile model
    model.compile(loss="mse",
                  optimizer=optimizer,
                  metrics=["mse"])

    history = model.fit(X_train, y_train, validation_split=.2, batch_size=32, epochs=10, verbose=2)

    # Evaluate our model
    obj_metric = history.history["val_loss"][-1] 
    return obj_metric

# COMMAND ----------

# MAGIC %md Now try experimenting with different hyperparameters + values!

# COMMAND ----------

# ANSWER

# NOTE: These are not the optimal hyperparameters, but just an example of how to use Hyperopt
space = {"dense_l1": hp.quniform("dense_l1", 10, 30, 1),
         "dense_l2": hp.quniform("dense_l2", 10, 30, 1),
         "learning_rate": hp.loguniform("learning_rate", -5, 0),
         "optimizer": hp.choice("optimizer", ["Adam"]),
        }

spark_trials = SparkTrials(parallelism=4)

best_hyperparams = fmin(run_nn, space, algo=tpe.suggest, max_evals=30, trials=spark_trials, rstate=np.random.default_rng(42))
best_hyperparams

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
