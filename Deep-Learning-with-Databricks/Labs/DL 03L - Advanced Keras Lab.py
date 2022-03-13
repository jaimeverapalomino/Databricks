# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Advanced Keras Lab
# MAGIC 
# MAGIC Now we are going to take the following objectives we learned in the past lab, and apply them here! You will further improve upon your first model with the wine quality dataset.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Generate a separate train/validation dataset
# MAGIC  - Perform data standardization
# MAGIC  - Create early stopping callback
# MAGIC  - Load and apply your saved model

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

import pandas as pd
from sklearn.datasets import load_wine

# Import Dataset
wine_quality = load_wine(as_frame=True)

# COMMAND ----------

from sklearn.model_selection import train_test_split

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(wine_quality.data,
                                                    wine_quality.target,
                                                    test_size=0.2,
                                                    random_state=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validation Data
# MAGIC 
# MAGIC In the demo notebook, we showed how to use the validation_split method to split your data into training and validation dataset.
# MAGIC 
# MAGIC Keras also allows you to specify a dedicated validation dataset.
# MAGIC 
# MAGIC Split your training set into 75-25 train-validation split. 

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Standardization
# MAGIC 
# MAGIC Go ahead and standardize our training, validation, and test features. 
# MAGIC 
# MAGIC Recap: Why do we want to standardize our features? Do we use the test statistics when computing the mean/standard deviation?

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC Let's use the same model architecture as in the previous lab.

# COMMAND ----------

import tensorflow as tf
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential
tf.random.set_seed(42)

def build_model():
    return Sequential([Dense(50, input_dim=13, activation="relu"),
                       Dense(20, activation="relu"),
                       Dense(1, activation="linear")])
  
model = build_model()
model.summary()

model.compile(optimizer="adam", loss="mse", metrics=["mse"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Callbacks
# MAGIC 
# MAGIC In the demo notebook, we covered how to implement the ModelCheckpoint callback (History is automatically done for us).
# MAGIC 
# MAGIC Now, add the model checkpointing, and only save the best model. Also add a callback for EarlyStopping (if the model doesn't improve after 2 epochs, terminate training). You will need to set **`patience=2`**, **`min_delta=.0001`**, and **`restore_best_weights=True`** to ensures the final model’s weights are from its best epoch, not just the last one.
# MAGIC 
# MAGIC Use the <a href="https://www.tensorflow.org/api_docs/python/tf/keras/callbacks/Callback" target="_blank">callbacks documentation</a> for reference!

# COMMAND ----------

# TODO
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping
 
filepath = f"{working_dir}/keras_checkpoint_weights_lab.ckpt".replace("dbfs:/", "/dbfs/")
 
checkpointer = FILL_IN
early_stopping = FILL_IN

# COMMAND ----------

# MAGIC %md ## 4. Fit Model
# MAGIC 
# MAGIC Now let's put everything together! Fit the model to the training and validation data **`(X_val, y_val)`** with **`epochs`**=30, **`batch_size`**=32, and the 2 callbacks we defined above: **`checkpointer`** and **`early_stopping`**.
# MAGIC 
# MAGIC Take a look at the <a href="https://www.tensorflow.org/api_docs/python/tf/keras/Model#fit" target="_blank">.fit()</a> method in the docs for help.

# COMMAND ----------

# TODO
history = model.fit(FILL_IN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load Model
# MAGIC 
# MAGIC Load in the weights saved from this model via checkpointing to a new variable called **`saved_model`**, and make predictions for our test data. Then compute the RMSE. See if you can do this without re-compiling the model!

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
