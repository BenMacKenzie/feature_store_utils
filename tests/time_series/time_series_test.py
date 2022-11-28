# Databricks notebook source
spark.sql('create schema if not exists feature_utils_test')
spark.sql('use feature_utils_test')


# COMMAND ----------

from pathlib import Path
from datetime import date, timedelta
import pandas as pd




beginDate = '2000-01-01'
endDate = '2050-12-31'
spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate").createOrReplaceTempView('dates')


dim_calendar=Path('dim_calendar.sql').read_text()
spark.sql(dim_calendar)


dates = [date(2019, 1, 1) + timedelta(n) for n in range(265)]
customers = [101, 102]

data = [[c, dates[d], 100*pow(1.05,d)] for c in customers for d in range(0,20)]
dbu_df = pd.DataFrame (data, columns = ['customer_id', 'date', 'dbu'])
spark.createDataFrame(dbu_df).write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('dbu')


# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table renewal_eol (
# MAGIC customer int,
# MAGIC renewal_date date,
# MAGIC churn boolean);
# MAGIC 
# MAGIC insert into table renewal_eol values (101, "2019-01-20", true);

# COMMAND ----------

from features.feature_generation import build_training_data_set,feature_tables, build_feature_table, register_dimension_table, tables 
df = build_training_data_set()

# COMMAND ----------

pdf = df.toPandas()
assert pdf.iloc[0]['6_day_geometric_growth_sql_dbu']==1.05
assert pdf.iloc[0]['6_day_average_growth_sql_dbu']==1.05

# COMMAND ----------



# COMMAND ----------


