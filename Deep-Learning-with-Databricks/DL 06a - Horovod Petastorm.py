# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Horovod with Petastorm
# MAGIC 
# MAGIC <a href="https://github.com/uber/petastorm" target="_blank">Petastorm</a> enables single machine or distributed training and evaluation of deep learning models from datasets in Apache Parquet format and datasets that are already loaded as Spark DataFrames. It supports ML frameworks such as TensorFlow, Pytorch, and PySpark and can be used from pure Python code.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Use Horovod to train a distributed neural network using Parquet files + Petastorm

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md ## Load data

# COMMAND ----------

from sklearn.datasets import fetch_california_housing
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import pandas as pd

cal_housing = fetch_california_housing()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(cal_housing.data,
                                                    cal_housing.target,
                                                    test_size=0.2,
                                                    random_state=1)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# COMMAND ----------

# MAGIC %md ## Spark DataFrame
# MAGIC 
# MAGIC Let's concatenate our features and label, then create a Spark DataFrame from our Pandas DataFrame.

# COMMAND ----------

data = pd.concat([pd.DataFrame(X_train, columns=cal_housing.feature_names), pd.DataFrame(y_train, columns=["label"])], axis=1)
train_df = spark.createDataFrame(data)
display(train_df)

# COMMAND ----------

# MAGIC %md ## Create Dense Vectors for Features

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

vec_assembler = VectorAssembler(inputCols=cal_housing.feature_names, outputCol="features")
vec_train_df = vec_assembler.transform(train_df).select("features", "label")
display(vec_train_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Convert the Spark DataFrame to a TensorFlow Dataset
# MAGIC In order to convert Spark DataFrames to a TensorFlow datasets, we need to do it in two steps:
# MAGIC <br><br>
# MAGIC 
# MAGIC 
# MAGIC 0. Define where you want to copy the data by setting Spark config
# MAGIC 0. Call make_spark_converter() method to make the conversion
# MAGIC 
# MAGIC This will copy the data to the specified path.

# COMMAND ----------

from petastorm.spark import SparkDatasetConverter, make_spark_converter

# Define directory the underlying files are copied to
path_to_cache = f"file:///{working_dir}/petastorm/"
dbutils.fs.rm(path_to_cache, recurse=True)
dbutils.fs.mkdirs(path_to_cache)

# Set the proper Spark config
spark.conf.set(SparkDatasetConverter.PARENT_CACHE_DIR_URL_CONF, path_to_cache)
# Make the conversion
converter_train = make_spark_converter(vec_train_df)

# COMMAND ----------

# MAGIC %md ## Define Model

# COMMAND ----------

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import models, layers
tf.random.set_seed(42)

def build_model():
    model = models.Sequential()
    model.add(layers.Dense(20, input_dim=8, activation="relu"))
    model.add(layers.Dense(20, activation="relu"))
    model.add(layers.Dense(1, activation="linear"))
    return model

# COMMAND ----------

# MAGIC %md ## Single Node
# MAGIC 
# MAGIC Define shape of the input tensor and output tensor and fit the model on the driver. We need to use Petastorm's <a href="https://petastorm.readthedocs.io/en/latest/api.html#petastorm.spark.spark_dataset_converter.SparkDatasetConverter.make_tf_dataset" target="_blank">make_tf_dataset</a> to read batches of data.<br><br>
# MAGIC 
# MAGIC * Note that we use **`num_epochs=None`** to generate infinite batches of data to avoid handling the last incomplete batch. This is particularly useful in the distributed training scenario, where we need to guarantee that the numbers of data records seen on all workers are identical. Given that the length of each data shard may not be identical, setting **`num_epochs`** to any specific number would fail to meet the guarantee.
# MAGIC * The **`workers_count`** param specifies the number of threads or processes to be spawned in the reader pool, and it is not a Spark worker. 

# COMMAND ----------

BATCH_SIZE = 32
NUM_EPOCH = 10
INITIAL_LR = 0.001

with converter_train.make_tf_dataset(workers_count=4, batch_size=BATCH_SIZE, num_epochs=None) as train_dataset:
    dataset = train_dataset.map(lambda x: (x.features, x.label))

    # Number of steps required to go through one epoch
    steps_per_epoch = len(converter_train) // BATCH_SIZE
    model = build_model()
    optimizer = keras.optimizers.Adam(learning_rate=INITIAL_LR)
    model.compile(optimizer=optimizer, loss="mse", metrics=["mae"])
    # Do not specify the batch_size if your data is in the form of a dataset (since they generate batches)
    # Instead, use steps_per_epoch
    model.fit(dataset, steps_per_epoch=steps_per_epoch, epochs=NUM_EPOCH, verbose=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Horovod
# MAGIC 
# MAGIC We will take the code from above and scale the **`steps_per_epoch`** and the **`optimizer`** by **`hvd.size()`**.

# COMMAND ----------

import horovod.tensorflow.keras as hvd

checkpoint_dir = f"{working_dir}/petastorm_checkpoint_weights.ckpt"
dbutils.fs.rm(checkpoint_dir, True)

def run_training_horovod():
    # Horovod: initialize Horovod.
    hvd.init()
    with converter_train.make_tf_dataset(batch_size=BATCH_SIZE, num_epochs=None, cur_shard=hvd.rank(), shard_count=hvd.size()) as train_dataset:
        dataset = train_dataset.map(lambda x: (x.features, x.label))
        model = build_model()
        steps_per_epoch = len(converter_train) // (BATCH_SIZE*hvd.size())

        # Adding in Distributed Optimizer
        optimizer = tf.keras.optimizers.Adam(learning_rate=INITIAL_LR)
        optimizer = hvd.DistributedOptimizer(optimizer)

        model.compile(optimizer=optimizer, loss="mse", metrics=["mae"])

        # Adding in callbacks
        callbacks = [
            hvd.callbacks.BroadcastGlobalVariablesCallback(0),
            hvd.callbacks.MetricAverageCallback(),
            hvd.callbacks.LearningRateWarmupCallback(initial_lr=INITIAL_LR*hvd.size(), warmup_epochs=5, verbose=2),
            tf.keras.callbacks.ReduceLROnPlateau(monitor="loss", patience=10, verbose=2)
        ]

        if hvd.rank() == 0:
            callbacks.append(tf.keras.callbacks.ModelCheckpoint(checkpoint_dir.replace("dbfs:/", "/dbfs/"), monitor="loss", save_best_only=True))

        history = model.fit(dataset, steps_per_epoch=steps_per_epoch, epochs=NUM_EPOCH, callbacks=callbacks, verbose=2)

# COMMAND ----------

from sparkdl import HorovodRunner

hr = HorovodRunner(np=-1, driver_log_verbosity="all")
hr.run(run_training_horovod)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run on all workers

# COMMAND ----------

hr = HorovodRunner(np=spark.sparkContext.defaultParallelism, driver_log_verbosity="all")
hr.run(run_training_horovod)

# COMMAND ----------

# MAGIC %md Let's load & evaluate the model.

# COMMAND ----------

from tensorflow.keras.models import load_model

trained_model = load_model(checkpoint_dir.replace("dbfs:/", "/dbfs/"))
print(trained_model.summary())

trained_model.evaluate(X_test, y_test)

# COMMAND ----------

# MAGIC %md Let's <a href="https://petastorm.readthedocs.io/en/latest/api.html#petastorm.spark.spark_dataset_converter.SparkDatasetConverter.delete" target="_blank">delete</a> the cached files

# COMMAND ----------

converter_train.delete()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
