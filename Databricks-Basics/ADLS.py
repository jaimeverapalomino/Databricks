# Databricks notebook source
# MAGIC %md
# MAGIC # Tutorial BCI Datos ADLS y Create Database (deprecated)
# MAGIC 
# MAGIC En este tutorial veremos como crear una base de datos apuntando al montaje del storage, y como crear tabkas sobre dicha base de datos.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Referencias
# MAGIC - markdown https://docs.databricks.com/notebooks/notebooks-use.html#include-documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datos en Storage ADLS v2
# MAGIC 
# MAGIC La data estara en un Azure DataLake Storage (ADLS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instalar File Browser Azure Storage Explorer
# MAGIC 
# MAGIC este es un utilitario que sirve para manejar archivos en ADLS

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Instalar en desktop local
# MAGIC 
# MAGIC https://azure.microsoft.com/en-us/features/storage-explorer/

# COMMAND ----------

# MAGIC %md
# MAGIC Conectarse al Storage de la POC con opción "Open Connect" (icono enchufe), seleccionar "use a Connection string"
# MAGIC 
# MAGIC En campo "Connection String" copiar string de conexion de Azure Storage account 
# MAGIC   
# MAGIC En arbol se va a ubicar nuestro storage bajo:
# MAGIC 
# MAGIC - Local & Attaches
# MAGIC - - Storage Accounts
# MAGIC 
# MAGIC Aqui pueden usarlo como File Browser y subir archivos de datos 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Conectarse desde este Notebook (DataSci)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC El storage esta montado con protocolo ADLSv2 que permite  escritura

# COMMAND ----------

# MAGIC %fs ls /mnt/bcirisgcredcontain

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a crear Base de Datos apuntando ADLS
# MAGIC 
# MAGIC ref https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-database.html

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS risgcred_db LOCATION '/mnt/bcirisgcredcontain/risgcred_db/';

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora toda tabla que creemos en esta base de datos va a estar en ADLS por defecto

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE risgcred_db.fraudetest_tb (`id_cliente` STRING, `jen_fec_evt_neg` STRING, `jen_num_ope_evt` STRING, `jen_id_cnl` STRING, `jen_id_pdt` STRING, `jen_id_evt` STRING, `jen_id_sub_evt` STRING, `jen_med` STRING, `target` STRING, `inicio` STRING) 
# MAGIC USING parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table risgcred_db.fraudetest_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO risgcred_db.fraudetest_tb VALUES (
# MAGIC '4363799',
# MAGIC '2019-03-28 15:16:01.000000',
# MAGIC 'WW0929608943',
# MAGIC 'MOVIBCINAT',
# MAGIC 'INT',
# MAGIC 'LOGIN',
# MAGIC 'OK',
# MAGIC '10.10.10.2',
# MAGIC '0',
# MAGIC '2019-03-28 15:23:26.00000');

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE risgcred_db.fraudetest_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from risgcred_db.fraudetest_tb where id_cliente = '4363799'

# COMMAND ----------

# MAGIC %fs ls /mnt/bcirisgcredcontain/risgcred_db/fraudetest_tb

# COMMAND ----------

# MAGIC %md 
# MAGIC Tambien podemos listarlo desde Python con dbutils

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/bcirisgcredcontain/risgcred_db/fraudetest_tb"))

# COMMAND ----------

# MAGIC %md 
# MAGIC y ya podemos realizar dataframes usando spark

# COMMAND ----------

    dfparquet = spark.read.parquet("/mnt/bcirisgcredcontain/risgcred_db/fraudetest_tb")

# COMMAND ----------

dfparquet.show()