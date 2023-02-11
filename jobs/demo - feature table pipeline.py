# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Ideally this notebook would be generated with only SQL or python code without need to import a python package.  to do...

# COMMAND ----------

from features.feature_generation import update_feature_table, feature_tables


# COMMAND ----------

update_feature_table(feature_tables['dbu_growth'])
update_feature_table(feature_tables['customer_service_calls'])

