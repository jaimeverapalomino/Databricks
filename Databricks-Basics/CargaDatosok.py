# Databricks notebook source
# MAGIC %md
# MAGIC # Tutorial BCI Carga Datos Riesgo Crediticio
# MAGIC 
# MAGIC Este tutorial muestra buenas practicas de carga de datos.
# MAGIC 
# MAGIC Muestra carga de carpetas con archivos raw (ejemplo CSV) y despues su posterior traspaso a una tabla parquet en la base de datos que apunta a ADLS.
# MAGIC 
# MAGIC ![carga](files/images_doc/dbricks_cargaRaw.png)
# MAGIC 
# MAGIC Tambien muestra como pasar de unaa tabla Parquet a una tabla Delta (soporta ACID)

# COMMAND ----------

# MAGIC %md
# MAGIC Previamente se debio haber cargado archivos con opción Databricks, esto es opción de menu "Data", botón "add data"
# MAGIC 
# MAGIC Luego opción "Upload File" y cargar en en path
# MAGIC 
# MAGIC > /FileStore/tables/rawdbfs/
# MAGIC 
# MAGIC ![uploadfiles](files/images_doc/dbricks_UploadFiles.png)
# MAGIC 
# MAGIC Para subir carpetas se recomienda arrastrar la carpeta sobre vtna Databricks
# MAGIC 
# MAGIC </br>
# MAGIC >* nota importante: la data siempre debe quedar finalmente en ADLS (Storage), y por lo tanto las tablas de datos siempre deben apuntar al montaje ADLS.
# MAGIC </br>
# MAGIC </br>
# MAGIC > luego esta carga de archivos en DBFS es solo transitoria, una vez traspasados a la tabla destino, estos archivos deben ser eliminados.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Una vez subido los archivos, vamos a tomarlos desde este notebook y vamos a generar o cargar la tabla respectiva.

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a verificar que estan los archivos que hicimos upload

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/rawdbfs/

# COMMAND ----------

# MAGIC %md
# MAGIC Tambien lo podemos listar con Python y la libreria de databricks dbutils

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/rawdbfs/"))

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a ver las bases de datos que estamos manejando en Databricks, se pueden ver tambien con la opción de menu "Data".

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %md
# MAGIC Esta base de datos fue creada apuntando a un path del Storage ADLS (location)
# MAGIC 
# MAGIC Ahora toda tabla que creemos en esta base de datos va a estar en ADLS por defecto

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DATABASE risgcred_db

# COMMAND ----------

# MAGIC %md 
# MAGIC Vamos a crear una tabla Parquet a partir de una carga de archivos Raw.

# COMMAND ----------

# MAGIC %md
# MAGIC Creo dataframe leyendo de la carpeta RAW en formato csv, infiero schema, para luego escribirlo como tabla (parquet)

# COMMAND ----------

dfPath = "dbfs:/FileStore/tables/rawdbfs/tmpmargen_jul2020/"
dfCSV=spark.read.format("csv").option("header","true")\
  .option("inferSchema", "true").load(dfPath)

# COMMAND ----------

dfCSV.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Para efectos de la demo vamos a borrar la tabla

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS risgcred_db.tmp_margen_acme_tb

# COMMAND ----------

# MAGIC %md 
# MAGIC Solo la primera carga se crea la tabla.
# MAGIC 
# MAGIC Al guardarlo como tabla e indicar la base de datos ("brujulafinanciera_db."), entonces automaticamente lo guarda en ADLS, porque saca la ruta ADLS de la base de datos (location). Si no se indica nada entonces crea la tabla como parquet por defecto.

# COMMAND ----------

dfCSV.write.saveAsTable("risgcred_db.tmp_margen_acme_tb")

# COMMAND ----------

# MAGIC %sql SELECT * FROM risgcred_db.tmp_margen_acme_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM risgcred_db.tmp_margen_acme_tb

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver la estructura con que se creo la tabla. Aqui no aparece location (hacia ADLS) porque lo hereda de la base de datos por defecto.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CREATE TABLE risgcred_db.tmp_margen_acme_tb

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora aparece la tabla como subcarpeta en el montaje (que corresponde a ADLS)

# COMMAND ----------

# MAGIC %fs ls /mnt/bcirisgcredcontain/risgcred_db/

# COMMAND ----------

# MAGIC %md 
# MAGIC Y podemos verificar que esta en formato parquet, revisando los archivos que se crearon.

# COMMAND ----------

# MAGIC %fs ls /mnt/bcirisgcredcontain/risgcred_db/tmp_margen_acme_tb/

# COMMAND ----------

# MAGIC %md 
# MAGIC Con esto comprobamos que ya los archivos fueron cargados como datos en la nueva tabla
# MAGIC 
# MAGIC Para mantener el orden, vamos a borrar los archivos cargados, quedando limpia la carpeta RAW

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/tables/rawdbfs/tmpmargen_jul2020/

# COMMAND ----------

# MAGIC %md
# MAGIC Como es una tabla ya podemos incluso hacer INSERT con SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO risgcred_db.tmp_margen_acme_tb VALUES (19700328,28,"Alfa","XXX","CLASE A","28X","CUENTA Y","CREDITOS ACME","ZZ PLAZO","PRODUCTO Z",1234567890,"TRANSACCION ALFA",0.0000,0.0000,28.28);

# COMMAND ----------

# MAGIC %md
# MAGIC Refresh es importante sobre todo si hay otro procesos afuera agregando archivos o insertando datos, para tener data actualizada.

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE risgcred_db.tmp_margen_acme_tb

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM risgcred_db.tmp_margen_acme_tb WHERE Id_Periodo = 19700328

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM risgcred_db.tmp_margen_acme_tb 

# COMMAND ----------

# MAGIC %md
# MAGIC En las siguientes cargas se agrega la data solamente, ya no se crea la tabla.

# COMMAND ----------

dfPathAgo = "dbfs:/FileStore/tables/rawdbfs/tmpmargen_ago2020/"
dfSig=spark.read.format("csv").option("header","true")\
  .option("inferSchema", "true").load(dfPathAgo)

# COMMAND ----------

dfSig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC El write es del tipo append (agrega nueva data a la tabla)

# COMMAND ----------

dfSig.write.mode("append").saveAsTable("risgcred_db.tmp_margen_acme_tb")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM risgcred_db.tmp_margen_acme_tb

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a leer toda la data (parquet), y vamos a crear un tabla en delta.

# COMMAND ----------

# MAGIC %md
# MAGIC hay varias formas de leer toda la data en DataFrame
# MAGIC 
# MAGIC ref https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html#dataframe-faqs

# COMMAND ----------

dfAll01 = table("risgcred_db.tmp_margen_acme_tb")
dfAll01.count()


# COMMAND ----------

dfAll02 = spark.sql("select * from risgcred_db.tmp_margen_acme_tb")
dfAll02.count()

# COMMAND ----------

# MAGIC %md
# MAGIC incluso podemos leer la data directo de la carpeta que corresponde a la tabla.
# MAGIC 
# MAGIC ref https://docs.databricks.com/data/data-sources/read-parquet.html

# COMMAND ----------

dfAll03=spark.read.parquet("dbfs:/mnt/bcirisgcredcontain/risgcred_db/tmp_margen_acme_tb/")
dfAll03.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora creamos la tabla delta (termina en "_dtb") destino a partir del dataframe
# MAGIC 
# MAGIC ref https://docs.databricks.com/_static/notebooks/delta/optimize-scala.html

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS risgcred_db.tmp_margen_delta_dtb

# COMMAND ----------

dfAll01.write.format("delta").saveAsTable("risgcred_db.tmp_margen_delta_dtb")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM risgcred_db.tmp_margen_delta_dtb

# COMMAND ----------

# MAGIC %md
# MAGIC podemos revisar que la creo en ADLS montado

# COMMAND ----------

# MAGIC %fs ls /mnt/bcirisgcredcontain/risgcred_db/tmp_margen_delta_dtb/