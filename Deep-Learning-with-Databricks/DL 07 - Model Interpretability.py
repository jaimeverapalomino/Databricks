# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Model Interpretability
# MAGIC 
# MAGIC <img style="width:20%" src="https://files.training.databricks.com/images/Limes.jpg" > <Br>
# MAGIC No, we're not talking about limes.
# MAGIC 
# MAGIC We're talking about methods of interpreting models, namely:
# MAGIC   * <a href="https://github.com/marcotcr/lime" target="_blank">Local Interpretable Model-Agnostic Explanations</a> (LIME) and 
# MAGIC   * <a href="https://shap.readthedocs.io/en/latest" target="_blank">SHapley Additive exPlanations</a> (SHAP)
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Use LIME and SHAP to understand which features are most important in the model's prediction for that data point
# MAGIC  
# MAGIC   We will be using <a href="https://docs.databricks.com/libraries/notebooks-python-libraries.html#notebook-scoped-python-libraries" target="_blank">notebook-scoped</a> libraries to install lime (shap is already installed).

# COMMAND ----------

# MAGIC %pip install lime==0.2.0.1
# MAGIC %pip install tensorflow-datasets
# MAGIC %pip install numba==0.53

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

import tensorflow_datasets as tfds
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Load dataset
wine_quality_tfds = tfds.load("wine_quality", split="train", shuffle_files=False)
wine_quality_pdf = tfds.as_dataframe(wine_quality_tfds)
wine_quality_pdf.columns = wine_quality_pdf.columns.str.replace("features/", "")

# Split and scale dataset
X_train, X_test, y_train, y_test = train_test_split(wine_quality_pdf.drop("quality", axis=1),
                                                    wine_quality_pdf["quality"],
                                                    test_size=0.2,
                                                    random_state=1)
# Scale features
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# COMMAND ----------

# MAGIC %md Build Model

# COMMAND ----------

import tensorflow as tf
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential
tf.random.set_seed(42)

model = Sequential()
model.add(Dense(50, input_dim=11, activation="relu"))
model.add(Dense(20, activation="relu"))
model.add(Dense(1))

model.compile(optimizer="adam", loss="mse", metrics=["mse"])

history = model.fit(X_train, y_train, epochs=30, batch_size=32, verbose=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using LIME for Model Explanation
# MAGIC We can use the <a href= "https://github.com/marcotcr/lime" target="_blank">LIME</a> library to provide explanations of individual predictions.
# MAGIC 
# MAGIC ![](https://raw.githubusercontent.com/marcotcr/lime/master/doc/images/lime.png)

# COMMAND ----------

from lime.lime_tabular import LimeTabularExplainer

help(LimeTabularExplainer)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Interpreting Results
# MAGIC LIME explains the impact of each feature on the prediction, and tabular explainers need a training set. 
# MAGIC 
# MAGIC The reason for this is because we compute statistics on each feature (column). If the feature is numerical, we compute the mean and std, and discretize it into quartiles.

# COMMAND ----------

def model_predict(input):
    """
    Convert keras prediction output to LIME compatible form. (Needs to output a 1 dimensional array)
    """
    return model.predict(input).flatten()

# NOTE: In order to pass in categorical_features, they all need to be ints
explainer = LimeTabularExplainer(X_train, feature_names=wine_quality_pdf.drop("quality", axis=1).columns, mode="regression")

# COMMAND ----------

exp = explainer.explain_instance(X_test[0], model_predict, num_features=11)
print(f"True value: {y_test.values[0]}")
print(f"Model predicted value: {exp.predicted_value}")
print(f"Local predicted value: {exp.local_pred[0]}")

# COMMAND ----------

# MAGIC %md ## Which features are most important?

# COMMAND ----------

displayHTML(exp.as_html())

# COMMAND ----------

# MAGIC %md 
# MAGIC Positive impact: 
# MAGIC 0. **`pH`** 
# MAGIC 0. **`sulphates`** 
# MAGIC 0. **`total sulfur dioxide` **
# MAGIC 0. **`chlorides`**     
# MAGIC 0. **`volatile acidity`**   
# MAGIC 
# MAGIC Negative impact:
# MAGIC 0. **`density`**
# MAGIC 0. **`residual sugar`** 
# MAGIC 0. **`free sulfur dioxide`**
# MAGIC 0. **`citric acid`** 
# MAGIC 0. **`alcohol`**
# MAGIC 0. **`fixed acidity`**
# MAGIC 
# MAGIC Do these make sense?

# COMMAND ----------

display(exp.as_pyplot_figure().tight_layout())

# COMMAND ----------

# MAGIC %md Let's get those values as a list.

# COMMAND ----------

exp.as_list()

# COMMAND ----------

# MAGIC %md ## SHAP
# MAGIC 
# MAGIC SHAP <a href="https://github.com/slundberg/shap" target="_blank">SHapley Additive exPlanations</a> is another approach to explain the output of a machine learning model. See the <a href="http://papers.nips.cc/paper/7062-a-unified-approach-to-interpreting-model-predictions" target="_blank">SHAP NIPS</a> paper for details, and Christoph Molnar's book chapter on <a href="https://christophm.github.io/interpretable-ml-book/shapley.html" target="_blank">Shapley Values</a>.
# MAGIC 
# MAGIC ![](https://raw.githubusercontent.com/slundberg/shap/master/docs/artwork/shap_diagram.png)
# MAGIC 
# MAGIC Great <a href="https://blog.dominodatalab.com/shap-lime-python-libraries-part-1-great-explainers-pros-cons/" target="_blank">blog post</a> comparing LIME to SHAP. SHAP provides greater theoretical guarantees than LIME, but at the cost of additional compute. 

# COMMAND ----------

import shap

help(shap.GradientExplainer)

# COMMAND ----------

import matplotlib.pyplot as plt
import tensorflow as tf

shap.initjs()
shap_explainer = shap.GradientExplainer(model, X_train[:200])
base_value = model.predict(X_train).mean() # base value = average prediction
shap_values = shap_explainer.shap_values(X_test[0:1])
y_pred = model.predict(X_test[0:1])
print(f"Actual price: {y_test.values[0]}, Predicted price: {y_pred[0][0]}")
                   
# Saving to File b/c can't display IFrame directly in Databricks: https://github.com/slundberg/shap/issues/101
file_path = "/tmp/shap.html"
shap.save_html(file_path, shap.force_plot(base_value, 
                                          shap_values[0], 
                                          features=X_test[0:1],
                                          feature_names=wine_quality_pdf.drop("quality", axis=1).columns, 
                                          show=False))

# COMMAND ----------

# MAGIC %md ## Visualize
# MAGIC 
# MAGIC * Red pixels increase the model's output while blue pixels decrease the output.
# MAGIC 
# MAGIC Here's a great <a href="https://christophm.github.io/interpretable-ml-book/shapley.html" target="_blank">article</a> discussing how SHAP works under the hood.
# MAGIC 
# MAGIC From the <a href="https://proceedings.neurips.cc/paper/2017/file/8a20a8621978632d76c43dfd28b67767-Paper.pdf" target="_blank">original SHAP paper</a>:
# MAGIC > Base value is the value that would be predicted if we did not know any features for the current output.
# MAGIC 
# MAGIC In other words, it is the mean prediction. 
# MAGIC 
# MAGIC Red/Blue: Features that push the prediction higher (to the right) are shown in red, and those pushing the prediction lower are shown in blue.

# COMMAND ----------

import codecs

f = codecs.open(file_path, "r")
displayHTML(f.read())

# COMMAND ----------

# MAGIC %md The values on the bottom show the true values of **`X_test[0]`**.

# COMMAND ----------

import pandas as pd

pd.DataFrame(X_test[0], wine_quality_pdf.drop("quality", axis=1).columns, ["features"])

# COMMAND ----------

# MAGIC %md
# MAGIC This says that, overall, factors like **`pH`** had the most positive effect on predicted wine quality, while **`free sulfur dioxide`** had the most negative.
# MAGIC 
# MAGIC Let's see what the values corresponding to each feature are.

# COMMAND ----------

shap_features = pd.DataFrame(shap_values[0][0], wine_quality_pdf.drop("quality", axis=1).columns, ["features"])
shap_features.sort_values("features")

# COMMAND ----------

# MAGIC %md
# MAGIC Visualize feature importance summary

# COMMAND ----------

shap.summary_plot(shap_explainer.shap_values(X_train[0:200]), 
                  features=X_train[0:200],
                  feature_names=wine_quality_pdf.drop("quality", axis=1).columns)

# COMMAND ----------

# MAGIC %md **Question**: How similar are the LIME predictions to the SHAP predictions?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
