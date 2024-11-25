# Databricks notebook source
# MAGIC %md
# MAGIC #### Supppose I want to build a churn model for databricks.
# MAGIC #### let's look at the feature store.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Features are available.   Let's build a training data set.
# MAGIC ####Notice that feature lookups require a lookup_key and a timestamp_lookup_key (because these features change over time)

# COMMAND ----------

from databricks.feature_store.client import FeatureStoreClient
from databricks.feature_store.entities.feature_lookup import FeatureLookup

fs = FeatureStoreClient()


feature_lookups = [
    FeatureLookup(
        table_name="ben_churn_model.dbu_growth",
        feature_names=["6_month_growth_sql_dbu", "6_month_growth_job_dbu"],
        lookup_key="customer_id",
        timestamp_lookup_key = "observation_date"
    ),
    FeatureLookup(
        table_name="ben_churn_model.customer_service_calls",
        feature_names=["customer_service_count"],        
        lookup_key="customer_id",
        timestamp_lookup_key = "observation_date"
    ),
  
   FeatureLookup(
        table_name="ben_churn_model.customers",
        feature_names=["tier"],        
        lookup_key="customer_id",
        timestamp_lookup_key = "observation_date"
    )
      
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### I've defined my features.  What else to I need?  
# MAGIC ```
# MAGIC training_set = fs.create_training_set(
# MAGIC     df,
# MAGIC     feature_lookups=feature_lookups,
# MAGIC     label="commit",
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### I like to refer to the df in this api call as the EOL dataframe:  Entity, Observation Date, Label.

# COMMAND ----------

eol_df = spark.sql("select customer_id, to_date(dateadd(month, -3, renewal_date)) as observation_date, commit from ben_churn_model.salesforce where contract_length =3")

# COMMAND ----------

display(eol_df)

# COMMAND ----------

training_set = fs.create_training_set(
    eol_df,
    feature_lookups=feature_lookups,
    label="commit",
)
training_df = training_set.load_df()



# COMMAND ----------

display(training_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Great. But what if you don't have features available in the feature store?
# MAGIC ####Staring with an EOL dataframe can help you think about your model and makes it easier to generate features
# MAGIC ####You can even automate aspects of feature computation.

# COMMAND ----------

# MAGIC %sh
# MAGIC cat features.yaml

# COMMAND ----------

from features.feature_generation import build_training_data_set,feature_tables, build_feature_table, register_dimension_table, tables 
df = build_training_data_set()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####As a good citizen you want to share your features.
# MAGIC ####Don't just write your training data set to the feature store!
# MAGIC
# MAGIC 1. don't restrict customers to 3 year contracts.
# MAGIC 2. backfill history at a well-considered time density.

# COMMAND ----------

build_feature_table(feature_tables['customer_service_calls'], drop_existing=True)
build_feature_table(feature_tables['dbu_growth'], drop_existing=True)
register_dimension_table(tables['customers'])

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to test feature computation?

# COMMAND ----------


