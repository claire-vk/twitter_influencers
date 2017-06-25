-- Databricks notebook source
-- outputAOCB is an alias name that links databricks to mysql. You need a new name for every table made.
-- “clean_hours” is a table in the RDS filesystem, the schema needs to be prepopulated: tell me your schema and I can do this part for you
-- allopenclos at the bottom is the data I’m looking to transfer over to mysql
-- NEEDS TO BE IN Spark SQL, not PySpark
CREATE TABLE neu_word_cloud2
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:mysql://flaskest.csjkhjjygutf.us-east-1.rds.amazonaws.com:3306/flaskdb",
  dbtable "neu_word_cloud",
  user "flask",
  password "wizjysys"
)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT OVERWRITE TABLE neg_influence1
-- MAGIC SELECT * FROM neg_influence

-- COMMAND ----------

-- MAGIC 
-- MAGIC %sql
-- MAGIC INSERT OVERWRITE TABLE pos_influence1
-- MAGIC SELECT * FROM pos_influence

-- COMMAND ----------

-- MAGIC 
-- MAGIC %sql
-- MAGIC INSERT OVERWRITE TABLE neu_influence1
-- MAGIC SELECT * FROM neu_influence

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT OVERWRITE TABLE neg_word_cloud2
-- MAGIC SELECT * FROM neg_word_cloud

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT OVERWRITE TABLE pos_word_cloud2
-- MAGIC SELECT * FROM pos_word_cloud

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT OVERWRITE TABLE neu_word_cloud2
-- MAGIC SELECT * FROM neu_word_cloud

-- COMMAND ----------


