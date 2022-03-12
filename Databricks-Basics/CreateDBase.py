# Databricks notebook source
# MAGIC %md
# MAGIC # Tutorial BCI Datos ADLS y Create Database bajo nuevo estandard

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

# MAGIC %md vamos a montar con protocolo ADLSv2 que permite  escritura

# COMMAND ----------

# MAGIC %fs ls /mnt/bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a crear Base de Datos apuntando ADLS
# MAGIC 
# MAGIC ref https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-database.html

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bdrcred_test_db LOCATION '/mnt/bronze/bdrcred_test_db/';

# COMMAND ----------

# MAGIC %md se creo carpeta de bdatos en ADLS

# COMMAND ----------

# MAGIC %fs ls /mnt/bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora toda tabla que creemos en esta base de datos va a estar en ADLS por defecto

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bdrcred_test_db.fraudetest_tb (`id_cliente` STRING, `jen_fec_evt_neg` STRING, `jen_num_ope_evt` STRING, `jen_id_cnl` STRING, `jen_id_pdt` STRING, `jen_id_evt` STRING, `jen_id_sub_evt` STRING, `jen_med` STRING, `target` STRING, `inicio` STRING) 
# MAGIC USING parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table bdrcred_test_db.fraudetest_tb

# COMMAND ----------

# MAGIC %md se creo carpeta de la tabla en ADLS

# COMMAND ----------

# MAGIC %fs ls /mnt/bronze/bdrcred_test_db

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO bdrcred_test_db.fraudetest_tb VALUES (
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
# MAGIC REFRESH TABLE bdrcred_test_db.fraudetest_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bdrcred_test_db.fraudetest_tb where id_cliente = '4363799'

# COMMAND ----------

# MAGIC %fs ls /mnt/bronze/bdrcred_test_db/fraudetest_tb

# COMMAND ----------

# MAGIC %md 
# MAGIC Tambien podemos listarlo desde Python con dbutils

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/bronze/bdrcred_test_db/fraudetest_tb"))

# COMMAND ----------

# MAGIC %md 
# MAGIC y ya podemos realizar dataframes usando spark

# COMMAND ----------

dfparquet = spark.read.parquet("/mnt/bronze/bdrcred_test_db/fraudetest_tb")

# COMMAND ----------

dfparquet.show()