from pyspark.sql import SparkSession
import pytest
from datetime import date, timedelta
import pandas as pd
from pathlib import Path
import yaml
from features.feature_generation import build_training_data_set


@pytest.fixture
def spark() -> SparkSession:
    """
    Create a spark session. Unit tests don't have access to the spark global
    """
    return SparkSession.builder.getOrCreate()


def test_type1_lookup(spark):
    # Databricks notebook source
    spark.sql('create schema if not exists feature_utils_test')
    spark.sql('use feature_utils_test')

    spark.sql("create or replace table customers (customer_id int, email string)")
    spark.sql("insert into table customers values (101, 'ben.mackenzie@databricks.com')")
    spark.sql("insert into table customers values (102, 'benmackenzie@gmail.com')")

    
    spark.sql("create or replace table renewal_eol (customer_id int, renewal_date date, commit boolean);")
    spark.sql("insert into table renewal_eol values (101, \"2019-01-20\", true)")
    spark.sql("insert into table renewal_eol values (102, \"2022-01-20\", true)")

   

    with open('tests/type1_tables/features.yaml', "r") as stream:
        data_spec = yaml.safe_load(stream)

    df = build_training_data_set(data_spec)
    pdf = df.toPandas()
    assert pdf.iloc[0]['current_email_domain']=='DATABRICKS'
    assert pdf.iloc[1]['current_email_domain']=='GMAIL'








