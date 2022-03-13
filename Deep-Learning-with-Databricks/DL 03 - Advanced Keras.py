# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Advanced Keras
# MAGIC 
# MAGIC Congrats on building your first neural network! In this notebook, we will cover even more topics to improve your model building. After you learn the concepts here, you will apply them to the neural network you just created.
# MAGIC 
# MAGIC We will use the California Housing Dataset.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Perform data standardization for better model convergence
# MAGIC  - Add validation data
# MAGIC  - Generate model checkpointing/callbacks
# MAGIC  - Use TensorBoard

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

print(cal_housing.DESCR)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the distribution of our features.

# COMMAND ----------

import pandas as pd

pd.DataFrame(X_train, columns=cal_housing.feature_names).describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Standardization
# MAGIC 
# MAGIC Because our features are all on different scales, it's going to be more difficult for our neural network during training. Let's do feature-wise standardization.
# MAGIC 
# MAGIC We are going to use the <a href="http://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html" target="_blank">StandardScaler</a> from Sklearn, which will remove the mean (zero-mean) and scale to unit variance.
# MAGIC 
# MAGIC $$x' = \frac{x - \bar{x}}{\sigma}$$

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Keras Model

# COMMAND ----------

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense  
tf.random.set_seed(42)

model = Sequential([
    Dense(20, input_dim=8, activation="relu"),
    Dense(20, activation="relu"),
    Dense(1, activation="linear")
])

# COMMAND ----------

from tensorflow.keras.optimizers import Adam

model.compile(optimizer=Adam(learning_rate=0.001), loss="mse", metrics=["mse"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validation Data
# MAGIC 
# MAGIC Let's take a look at the <a href="https://www.tensorflow.org/api_docs/python/tf/keras/Sequential#fit" target="_blank">.fit()</a> method in the docs to see all of the options we have available! 
# MAGIC 
# MAGIC We can either explicitly specify a validation dataset, or we can specify a fraction of our training data to be used as our validation dataset.
# MAGIC 
# MAGIC The reason why we need a validation dataset is to evaluate how well we are performing on unseen data (neural networks will overfit if you train them for too long!).
# MAGIC 
# MAGIC We can specify **`validation_split`** to be any value between 0.0 and 1.0 (defaults to 0.0).

# COMMAND ----------

history = model.fit(X_train, y_train, validation_split=.2, epochs=10, batch_size=64, verbose=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Checkpointing
# MAGIC 
# MAGIC After each epoch, we want to save the model. However, we will pass in the flag **`save_best_only=True`**, which will only save the model if the validation loss decreased. This way, if our machine crashes or we start to overfit, we can always go back to the "good" state of the model.
# MAGIC 
# MAGIC To accomplish this, we will use the ModelCheckpoint <a href="https://www.tensorflow.org/api_docs/python/tf/keras/callbacks/ModelCheckpoint" target="_blank">callback</a>. History is an example of a callback that is automatically applied to every Keras model.

# COMMAND ----------

from tensorflow.keras.callbacks import ModelCheckpoint

filepath = f"{working_dir}/keras_checkpoint_weights.ckpt".replace("dbfs:/", "/dbfs/")

model_checkpoint = ModelCheckpoint(filepath=filepath, verbose=1, save_best_only=True)

# COMMAND ----------

# MAGIC %md ## 4. Tensorboard
# MAGIC 
# MAGIC Tensorboard provides a nice UI to visualize the training process of your neural network and can help with debugging! We can define it as a callback.
# MAGIC 
# MAGIC Here are links to Tensorboard resources:
# MAGIC * <a href="https://www.tensorflow.org/tensorboard/get_started" target="_blank">Getting Started with Tensorboard</a>
# MAGIC * <a href="https://www.tensorflow.org/tensorboard/tensorboard_profiling_keras" target="_blank">Profiling with Tensorboard</a>
# MAGIC * <a href="https://www.datacamp.com/community/tutorials/tensorboard-tutorial" target="_blank">Effects of Weight Initialization</a>
# MAGIC 
# MAGIC 
# MAGIC Here is a <a href="https://databricks.com/blog/2020/08/25/tensorboard-a-new-way-to-use-tensorboard-on-databricks.html" target="_blank">Databricks blog post</a> that contains an end-to-end example of using Tensorboard on Databricks. 

# COMMAND ----------

# MAGIC %load_ext tensorboard

# COMMAND ----------

log_dir = f"/tmp/{username}"

# COMMAND ----------

# MAGIC %md
# MAGIC We just cleared out the log directory above in case you re-run this notebook multiple times.

# COMMAND ----------

# MAGIC %tensorboard --logdir $log_dir

# COMMAND ----------

# MAGIC %md Now let's add in our model checkpoint and Tensorboard callbacks to our **`.fit()`** command.
# MAGIC 
# MAGIC Click the refresh button on Tensorboard to view the Tensorboard output when the training has completed.

# COMMAND ----------

from tensorflow.keras.callbacks import TensorBoard

### Here, we set histogram_freq=1 so that we can visualize the distribution of a Tensor over time. 
### It can be helpful to visualize weights and biases and verify that they are changing in an expected way. 
### Refer to the Tensorboard documentation linked above.

tensorboard = TensorBoard(log_dir, histogram_freq=1)
history = model.fit(X_train, y_train, validation_split=.2, epochs=10, batch_size=64, verbose=2, callbacks=[model_checkpoint, tensorboard])

# COMMAND ----------

model.evaluate(X_test, y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Go back and click to refresh the Tensorboard! Note that under the **`histograms`** tab, you will see histograms of **`kernel`** in each layer; **`kernel`** represents the weights of the neural network.
# MAGIC 
# MAGIC If you are curious about how different initial weight initialization methods affect the training of neural networks, you can change the default weight initialization within the first Dense layer of your neural network. This <a href="https://www.tensorflow.org/api_docs/python/tf/keras/initializers" target="_blank">documentation</a> lists all the types of weight initialization methods that are supported by Tensorflow.
# MAGIC 
# MAGIC <pre><code><strong>model = Sequential([
# MAGIC     Dense(20, input_dim=8, activation="relu", kernel_initializer="<insert_different_weight_initialization_methods_here>"),
# MAGIC     Dense(20, activation="relu"),
# MAGIC     Dense(1, activation="linear")
# MAGIC ])
# MAGIC </strong></code></pre>
# MAGIC 
# MAGIC If you would like to share your Tensorboard result with your peer, you can check out <a href="https://tensorboard.dev/" target="_blank">TensorBoard.dev</a> (currently in preview) that allows you to share the dashboard. All you need to do is to upload your Tensorboard logs. 

# COMMAND ----------

# MAGIC %md
# MAGIC Now, head over to the lab to apply the techniques you have learned to the Wine Quality dataset! 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
