
import pytest
from datetime import date, timedelta
import pandas as pd
from pathlib import Path
import yaml
from features.sql_gen import load, get_sql_for_feature


def test_type1():
    with open('tests/local_tests/features.yaml', "r") as stream:
        data_spec = yaml.safe_load(stream)

    load(data_spec)
    select_clause = get_sql_for_feature('current_email_domain')
    print(select_clause)



test_type1()   


  
  