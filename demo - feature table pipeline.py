# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Ideally this notebook would be generated with only SQL or python code without need to import a python package.  to do...

# COMMAND ----------

from features.feature_generation import build_feature_table, feature_tables


# COMMAND ----------

build_feature_table(feature_tables['dbu_growth'], drop_existing=False, update=True)
build_feature_table(feature_tables['customer_service_calls'], drop_existing=False, update=True)

