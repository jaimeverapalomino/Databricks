# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Horovod
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Use Horovod to train a distributed neural network
# MAGIC   
# MAGIC HorovodRunner is a general API to run distributed DL workloads on Databricks using Uber’s <a href="https://github.com/uber/horovod" target="_blank">Horovod</a> framework. By integrating Horovod with Spark’s barrier mode, Databricks is able to provide higher stability for long-running deep learning training jobs on Spark. HorovodRunner takes a Python method that contains DL training code with Horovod hooks. This method gets pickled on the driver and sent to Spark workers. A Horovod MPI job is embedded as a Spark job using barrier execution mode. The first executor collects the IP addresses of all task executors using BarrierTaskContext and triggers a Horovod job using mpirun. Each Python MPI process loads the pickled program back, deserializes it, and runs it.
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/horovod-runner.png)
# MAGIC 
# MAGIC For additional resources, see:
# MAGIC * <a href="https://docs.microsoft.com/en-us/azure/databricks/applications/deep-learning/distributed-training/horovod-runner" target="_blank">Horovod Runner Docs</a>
# MAGIC * <a href="https://vimeo.com/316872704/e79235f62c" target="_blank">Horovod Runner webinar</a>  

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md ## Build Model

# COMMAND ----------

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
tf.random.set_seed(42)

def build_model():
    return Sequential([Dense(20, input_dim=8, activation="relu"),
                       Dense(20, activation="relu"),
                       Dense(1, activation="linear")]) # Keep the output layer as linear because this is a regression problem

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shard Data
# MAGIC 
# MAGIC From the <a href="https://github.com/horovod/horovod/blob/master/docs/concepts.rst" target="_blank">Horovod docs</a>:
# MAGIC 
# MAGIC Horovod core principles are based on the MPI concepts size, rank, local rank, allreduce, allgather, and broadcast. These are best explained by example. Say we launched a training script on 4 VMs, each having 4 GPUs. If we launched one copy of the script per GPU:
# MAGIC 
# MAGIC * Size would be the number of processes, in this case, 16.
# MAGIC 
# MAGIC * Rank would be the unique process ID from 0 to 15 (size - 1).
# MAGIC 
# MAGIC * Local rank would be the unique process ID within the VM from 0 to 3.
# MAGIC 
# MAGIC We need to shard our data across our processes.  **NOTE:** We are using a Pandas DataFrame for demo purposes. In the next notebook we will use Parquet files with Petastorm for better scalability.

# COMMAND ----------

from sklearn.datasets import fetch_california_housing
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

def get_dataset(rank=0, size=1):
    scaler = StandardScaler()
    cal_housing = fetch_california_housing(data_home=f"{working_dir}/{rank}/".replace("dbfs:/", "/dbfs/"))
    X_train, X_test, y_train, y_test = train_test_split(cal_housing.data,
                                                        cal_housing.target,
                                                        test_size=0.2,
                                                        random_state=1)
    scaler.fit(X_train)
    X_train = scaler.transform(X_train[rank::size])
    y_train = y_train[rank::size]
    X_test = scaler.transform(X_test[rank::size])
    y_test = y_test[rank::size]
    return (X_train, y_train), (X_test, y_test)

# COMMAND ----------

# MAGIC %md ## Horovod

# COMMAND ----------

import horovod.tensorflow.keras as hvd
from tensorflow.keras import optimizers

def run_training_horovod():
    # Horovod: initialize Horovod
    hvd.init()
    #     # If using GPU: pin GPU to be used to process local rank (one GPU per process)
    #   gpus = tf.config.experimental.list_physical_devices('GPU')
    #   print(gpus)
    #   for gpu in gpus:
    #       tf.config.experimental.set_memory_growth(gpu, True)
    #   if gpus:
    #       tf.config.experimental.set_visible_devices(gpus[hvd.local_rank()], 'GPU')

    print(f"Rank is: {hvd.rank()}")
    print(f"Size is: {hvd.size()}")

    (X_train, y_train), (X_test, y_test) = get_dataset(hvd.rank(), hvd.size())

    model = build_model()

    # Horovod: adjust learning rate based on number of GPUs/CPUs
    optimizer = optimizers.Adam(learning_rate=0.001*hvd.size())

    # Horovod: add Horovod Distributed Optimizer
    optimizer = hvd.DistributedOptimizer(optimizer)

    model.compile(optimizer=optimizer, loss="mse", metrics=["mse"])

    history = model.fit(X_train, y_train, validation_split=.2, epochs=10, batch_size=64, verbose=2)

# COMMAND ----------

# MAGIC %md Test it out on just the driver (negative sign indicates running on the driver).

# COMMAND ----------

from sparkdl import HorovodRunner

hr = HorovodRunner(np=-1, driver_log_verbosity="all")
hr.run(run_training_horovod)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Callbacks

# COMMAND ----------

from tensorflow.keras import optimizers
from tensorflow.keras.callbacks import ReduceLROnPlateau, ModelCheckpoint

checkpoint_dir = f"{working_dir}/horovod_checkpoint_weights.ckpt".replace("dbfs:/", "/dbfs/")

def run_training_horovod():
    # Horovod: initialize Horovod.
    hvd.init()

    # If using GPU: pin GPU to be used to process local rank (one GPU per process)
    #   gpus = tf.config.experimental.list_physical_devices('GPU')
    #   for gpu in gpus:
    #       tf.config.experimental.set_memory_growth(gpu, True)
    #   if gpus:
    #       tf.config.experimental.set_visible_devices(gpus[hvd.local_rank()], 'GPU')

    print(f"Rank is: {hvd.rank()}")
    print(f"Size is: {hvd.size()}")

    (X_train, y_train), (X_test, y_test) = get_dataset(hvd.rank(), hvd.size())

    model = build_model()

    # Horovod: adjust learning rate based on number of GPUs.
    optimizer = optimizers.Adam(learning_rate=0.001)

    # Horovod: add Horovod Distributed Optimizer.
    optimizer = hvd.DistributedOptimizer(optimizer)

    model.compile(optimizer=optimizer, loss="mse", metrics=["mse"])

    callbacks = [
        # Horovod: broadcast initial variable states from rank 0 to all other processes.
        # This is necessary to ensure consistent initialization of all workers when
        # training is started with random weights or restored from a checkpoint.
        hvd.callbacks.BroadcastGlobalVariablesCallback(0),

        # Horovod: average metrics among workers at the end of every epoch.
        # Note: This callback must be in the list before the ReduceLROnPlateau,
        # TensorBoard or other metrics-based callbacks.
        hvd.callbacks.MetricAverageCallback(),

        # Horovod: using `lr = 1.0 * hvd.size()` from the very beginning leads to worse final
        # accuracy. Scale the learning rate `lr = 1.0` ---> `lr = 1.0 * hvd.size()` during
        # the first five epochs. See https://arxiv.org/abs/1706.02677 for details.
        hvd.callbacks.LearningRateWarmupCallback(initial_lr=0.001*hvd.size(), warmup_epochs=5, verbose=1),

        # Reduce the learning rate if training plateaus.
        ReduceLROnPlateau(patience=10, verbose=1)
    ]

    # Horovod: save checkpoints only on worker 0 to prevent other workers from corrupting them.
    if hvd.rank() == 0:
        callbacks.append(ModelCheckpoint(checkpoint_dir, save_best_only=True))

    history = model.fit(X_train, y_train, validation_split=.2, epochs=10, batch_size=64, verbose=2, callbacks=callbacks)

# COMMAND ----------

# MAGIC %md Test it out on just the driver.

# COMMAND ----------

hr = HorovodRunner(np=-1, driver_log_verbosity="all")
hr.run(run_training_horovod)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run on all workers

# COMMAND ----------

## OPTIONAL: You can enable Horovod Timeline as follows, but can incur slow down from frequent writes, and have to export out of Databricks to upload to chrome://tracing
# import os
# os.environ["HOROVOD_TIMELINE"] = f"{working_dir}/_timeline.json"

hr = HorovodRunner(np=spark.sparkContext.defaultParallelism, driver_log_verbosity="all") # Set to default parallelism
hr.run(run_training_horovod)

# # If running on GPUs
# physical_devices = tf.config.list_physical_devices("GPU")
# hr = HorovodRunner(np=len(pyhsical_devices), driver_log_verbosity="all")
# hr.run(run_training_horovod)

# COMMAND ----------

# MAGIC %md
# MAGIC Load model from checkpoint for inference.

# COMMAND ----------

from tensorflow.keras.models import load_model

trained_model = load_model(checkpoint_dir)
trained_model.summary()

# COMMAND ----------

X, y = get_dataset()[1]
trained_model.evaluate(X, y)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
