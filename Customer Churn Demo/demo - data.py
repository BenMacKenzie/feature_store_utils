# Databricks notebook source
# MAGIC %md
# MAGIC need file support.  run this notebook on DBR ML 11.2 or higher.

# COMMAND ----------

pip install -r ../requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC use benmackenzie_catalog.churn_model

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's look at the data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers order by customer_id

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dbu order by customer_id, date desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_support;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build a churn model
# MAGIC ##### orient around renewal date
# MAGIC ##### model is for customers on 3 year contracts only.
# MAGIC ##### features: job and sql dbu growth, number of interactions with customer support, tier.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesforce order by customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view renewal_eol as select customer_id, to_date(dateadd(month, -3, renewal_date)) as observation_date, commit from salesforce where contract_length =3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from renewal_eol order by customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build features based on yaml spec

# COMMAND ----------


import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '..')))

from features.feature_generation import build_training_data_set




# COMMAND ----------

df = build_training_data_set()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Now lets create the corresponding feature tables
# MAGIC ##### note that the training data set should NOT be used.   Look at feature table spec in yaml file.  Need to consider backfill, time density of features

# COMMAND ----------

from features.feature_generation import build_feature_table
build_feature_table('customer_service_calls', drop_existing=False)

# COMMAND ----------

build_feature_table('dbu_growth', drop_existing=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### handle dimension tables differently.  Just register the underlying table with feature store

# COMMAND ----------

from features.feature_generation import register_dimension_table
from features.feature_spec import get_tables
tables = get_tables()
register_dimension_table(tables['customers'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### observe that now we can build the same training data set using the feature store directly
# MAGIC ** fix *** email_domain is not in customers table, so can't just register it as is.

# COMMAND ----------

from databricks.feature_store.client import FeatureStoreClient
from databricks.feature_store.entities.feature_lookup import FeatureLookup

fs = FeatureStoreClient()


# COMMAND ----------

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
renewal_eol_df = spark.sql('select * from renewal_eol')

training_set = fs.create_training_set(
    renewal_eol_df,
    feature_lookups=feature_lookups,
    label="commit",
)
training_df = training_set.load_df()

# COMMAND ----------

display(training_df)
