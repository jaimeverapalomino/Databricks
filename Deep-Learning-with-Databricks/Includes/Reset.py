# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# Does any work to reset the environment prior to testing.
username = spark.sql("SELECT current_user()").first()[0]

course_dir = f"dbfs:/user/{username}/dbacademy/deep_learning"

print(f"Removing course directory: {course_dir}")
dbutils.fs.rm(course_dir, True)

# COMMAND ----------

try: 
    dbutils.fs.unmount("/mnt/training")
    print("/mnt/training was unmouted")
except: 
    print("/mnt/training was not mounted")

# COMMAND ----------

# MAGIC %run "./Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
