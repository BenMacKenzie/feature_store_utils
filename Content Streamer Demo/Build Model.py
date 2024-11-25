# Databricks notebook source
# MAGIC %sql
# MAGIC use benmackenzie_catalog.fs_content_streamer_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ###We are going to build a churn model for a streaming service. 
# MAGIC
# MAGIC ###Let's look at our Data

# COMMAND ----------

# MAGIC %md
# MAGIC ###Customer table is type 2.  
# MAGIC The subscription and region are potential features.  Start date might have some signal if we can convert it to a duration

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers order by customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### customer service calls is an aggregation of interactions with customer service
# MAGIC (tbh...not probably realistic for this use case)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_service_calls order by customer_id, observation_date

# COMMAND ----------

# MAGIC %md
# MAGIC ###View aggregates represents 30 windows of aggregated activity

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from viewing_aggregate

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create our model.  First...the EOL table.  What entities do we want to include?  How do we want to orient the observation date?

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from subscription_history

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view renewal_eol as select customer_id, to_date(dateadd(month, -2, renewal_date)) as observation_date, renew from subscription_history where contract_length = 'annual'

# COMMAND ----------

renewal_eol = spark.read.table('renewal_eol')
display(renewal_eol)

# COMMAND ----------

# MAGIC %md
# MAGIC I don't want to create a feature that represents number of days a customer has been a subscriber..I'll just calculate this on-the-fly

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace FUNCTION benmackenzie_catalog.fs_content_streamer_demo.subscription_length(start date, today date)
# MAGIC RETURNS double
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'number of days since original subscription '
# MAGIC AS $$
# MAGIC from datetime import date
# MAGIC def subtract(n1: date, n2: date) -> int:
# MAGIC     delta = n2 - n1
# MAGIC     return(delta.days)
# MAGIC
# MAGIC return subtract(start, today)
# MAGIC $$

# COMMAND ----------

pip install  databricks-feature-store

# COMMAND ----------

from databricks.feature_engineering.entities.feature_lookup import FeatureLookup
from databricks.feature_engineering import FeatureEngineeringClient, FeatureFunction
fs = FeatureEngineeringClient()

feature_lookups = [
    FeatureLookup(
        table_name="fs_content_streamer_demo.viewing_aggregate",
        feature_names=["30_day_total_num_logins", "30_day_total_titles_viewed",
                       "30_day_total_titles_completed", "30_day_total_minutes_viewed"],
        lookup_key="customer_id",
        timestamp_lookup_key = "observation_date"
    ),
    FeatureLookup(
        table_name="fs_content_streamer_demo.customer_service_calls",
        feature_names=["30_day_customer_support_contact"],        
        lookup_key="customer_id",
        timestamp_lookup_key = "observation_date"
    ),
  
   FeatureLookup(
        table_name="fs_content_streamer_demo.customers",
        feature_names=["subscription", "initial_subscription_date", "region"],        
        lookup_key="customer_id",
        timestamp_lookup_key = "observation_date"
    ),
    FeatureFunction(
        udf_name="fs_content_streamer_demo.subscription_length",    # UDF must be in Unity Catalog so uses a three-level namespace
        input_bindings={
            "start": "initial_subscription_date",
            "today": "observation_date"
        },
        output_name="num_days_since_initial_subscription")
      
]
renewal_eol_df = spark.sql('select * from renewal_eol')

training_set = fs.create_training_set(
    df=renewal_eol_df,
    feature_lookups=feature_lookups,
    label = 'renew',
    exclude_columns = ['customer_id', 'observation_date', 'initial_subscription_date']
)
training_df = training_set.load_df()

# COMMAND ----------

display(training_df)

# COMMAND ----------


