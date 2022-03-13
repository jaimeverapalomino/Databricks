# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Agenda
# MAGIC ## Deep Learning with Databricks

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Day 1 AM
# MAGIC | Time | Lesson &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 30m  | **Introductions & Setup**                               | *Registration, Courseware & Q&As* |
# MAGIC | 25m  | **[Linear Regression]($./DL 01 - Linear Regression)** | Build a linear regression model using Sklearn and reimplement it in Keras </br> Modify # of epochs </br> Visualize loss | 
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 30m  | **[Keras]($./DL 02 - Keras)**  | Modify these parameters for increased model performance: activation functions, loss functions, optimizer, batch size </br> Save and load models |
# MAGIC | 25m    | **[Keras Lab]($./Labs/DL 02L - Keras Lab)**    | Build and evaluate your first Keras model! |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 50m  | **[Advanced Keras]($./DL 03 - Advanced Keras)** | Perform data standardization for better model convergence </br> Add validation data </br> Generate model checkpointing/callbacks </br> Use TensorBoard </br> Apply dropout regularization |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 35m  | **[Advanced Keras Lab]($./Labs/DL 03L - Advanced Keras Lab)**      | Practice using checkpoints and callbacks
# MAGIC | 20m |**Introduction to [MLflow]($./DL 04 - MLflow)**| MLflow introduction |

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Day 1 PM
# MAGIC | Time | Lesson &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 20m  | **Review**                               | *Review of Day 1 AM* |
# MAGIC | 35m  |**[MLflow]($./DL 04 - MLflow)** | Log experiments with MLflow</br> View MLflow UI</br> Generate a UDF with MLflow and apply to a Spark DataFrame |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 25m  | **[MLflow Lab]($./Labs/DL 04L - MLflow Lab)**| Log experiments with MLflow</br> View MLflow UI</br> Generate a UDF with MLflow and apply to a Spark DataFrame |
# MAGIC | 30m  | **[HyperOpt]($./DL 05 - Hyperopt)** | Use HyperOpt with SparkTrials to perform distributed hyperparameter search |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 30m  | **[HyperOpt Lab]($./Labs/DL 05L - Hyperopt Lab)** | Use HyperOpt with SparkTrials to perform distributed hyperparameter search |
# MAGIC | 25m  | **Introduction to [Horovod]($./DL 06 - Horovod)** | Horovod concept |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 45m  | **[Horovod]($./DL 06 - Horovod)** | Use Horovod to train a distributed neural network </br> Distributed Deep Learning best practices |

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Day 2 AM
# MAGIC | Time | Lesson &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 20m  | **Review**                               | *Review of Day 1* |
# MAGIC | 30m  | **[Horovod Petastorm]($./DL 06a - Horovod Petastorm)** | Use Horovod to train a distributed neural network using Parquet files + Petastorm|
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 35m  | **[Horovod Lab]($./Labs/DL 06L - Horovod Lab)** | Prepare your data for use with Horovod</br> Distribute the training of our model using HorovodRunner</br> Use Parquet files as input data for our distributed deep learning model with Petastorm + Horovod | 
# MAGIC | 25m  | **[Model Interpretability]($./DL 07 - Model Interpretability)**  | Use LIME and SHAP to understand which features are most important in the model's prediction for that data point |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 30m    | **[Distributed Inference with CNNs]($./DL 08 - Distributed Inference with CNNs)**    | Analyze popular CNN architectures </br> Apply pre-trained CNNs to images using Pandas Scalar Iterator UDF |
# MAGIC | 30m  | **[Shap for CNN Lab]($./Labs/DL 08L - SHAP for CNNs Lab)**  | Use SHAP to generate explanation behind a model's predictions |
# MAGIC | 10m  | **Break**                                               ||
# MAGIC | 35m  | **[Transfer Learning for CNNs]($./DL 09 - Transfer Learning for CNNs)**  | Perform transfer learning to create a cat vs dog classifier |

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Day 2 PM
# MAGIC | Time | Lesson &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 20m  | **Review**                               | *Review of Day 2 AM* |
# MAGIC | 30m  | **[Model Serving]($./DL 10 - Model Serving)**  | Real time deployment of a convolutional neural network using REST and Databricks MLflow Model Serving |
# MAGIC | 10m  | **Break** | ||
# MAGIC | 50m  | **[Embeddings]($./DL 11 - Embeddings)**  | Understand what embeddings are and how to use them |
# MAGIC | 10m  | **Break** | ||
# MAGIC | 50m  | **[Transfer Learning for NER]($./DL 12 - Transfer Learning for NER)**  | Fine-tune a pretrained model to solve named entity recognition|
# MAGIC | 10m  | **Break** | ||
# MAGIC | 30m  | **[CNN Lab]($./Labs/DL 09L - Transfer Learning for CNNs Lab) or [NLP Lab]($./Labs/DL 12L - Transfer Learning for Document Classification)**  | Apply transfer learning to classify pneumonial X-ray images or tweet sentiments|

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
