# Databricks notebook source
# MAGIC %md
# MAGIC Feature store is not compatible with mlfow 2.0.  Run this notebook on 10.4 LTS ML cluster

# COMMAND ----------

# MAGIC %md
# MAGIC adding a new cell

# COMMAND ----------

# MAGIC %sql
# MAGIC use ben_churn_model

# COMMAND ----------

eol_df = spark.sql('select customer_id, to_date(dateadd(month, -2, renewal_date)) as renewal_date, case when commit then 1 else 0 end as commit from salesforce where contract_length =3')

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
        timestamp_lookup_key = "renewal_date"
    ),
    FeatureLookup(
        table_name="ben_churn_model.customer_service_calls",
        feature_names=["customer_service_count"],        
        lookup_key="customer_id",
        timestamp_lookup_key = "renewal_date"
    ),
  
   FeatureLookup(
        table_name="ben_churn_model.customers",
        feature_names=["tier"],        
        lookup_key="customer_id",
        timestamp_lookup_key = "renewal_date"
    )
      
]
#renewal_eol_df = spark.sql('select * from renewal_eol')

training_set = fs.create_training_set(
    eol_df,
    feature_lookups = feature_lookups,
    label = 'commit',
    exclude_columns = ['customer_id', 'renewal_date']
  )

# COMMAND ----------

display(training_set.load_df())

# COMMAND ----------

import mlflow
import sklearn
from sklearn import set_config
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeClassifier

transformers = []

one_hot_pipeline = Pipeline(steps=[
    ("one_hot_encoder", OneHotEncoder(handle_unknown="ignore")),
])

transformers.append(("onehot", one_hot_pipeline, ["tier"]))
preprocessor = ColumnTransformer(transformers, remainder="passthrough", sparse_threshold=0)

skdtc_classifier = DecisionTreeClassifier(
  criterion="gini",
  max_depth=10
)

model = Pipeline([
    ("preprocessor", preprocessor),
    ("classifier", skdtc_classifier),
])

with mlflow.start_run():

  # df has columns ['customer_id', 'product_id', 'rating']
  

  training_df = training_set.load_df().toPandas()

 
  X_train = training_df.drop(['commit'], axis=1)
  y_train = training_df.commit

  model.fit(X_train, y_train)

  fs.log_model(
    model,
    "renewal_model",
    flavor=mlflow.sklearn,
    training_set=training_set,
    registered_model_name="renewal_model"
  )

# COMMAND ----------

inference_df = spark.sql('select distinct(customer_id) as customer_id, current_date() as renewal_date from salesforce where contract_length =3')

# COMMAND ----------

display(inference_df)

# COMMAND ----------

scored = fs.score_batch(
  f"models:/renewal_model/2",
  inference_df
)


# COMMAND ----------

display(scored)

# COMMAND ----------


