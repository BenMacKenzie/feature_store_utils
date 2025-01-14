from datetime import date, timedelta
from pathlib import Path
import yaml
from features.sql_gen import get_sql_for_feature
from features.feature_spec import load


def test_type1():
    with open('tests/local_tests/features_1.yaml', "r") as stream:
        data_spec = yaml.safe_load(stream)

    load(data_spec)
    select_clause = get_sql_for_feature('current_email_domain')
    print(select_clause)

def test_fact_aggregate():
    with open('tests/local_tests/features_2.yaml', "r") as stream:
        data_spec = yaml.safe_load(stream)

    load(data_spec)
    select_clause = get_sql_for_feature('total_minutes_watched_prior_month')
    print(select_clause)
    

#test_type1()   

test_fact_aggregate()
  
  