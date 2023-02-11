import yaml
from datetime import datetime
from jinja2 import Environment, PackageLoader
from dotenv import find_dotenv
import os
from features.feature_spec import get_features, get_tables, get_data_spec
from features.feature_spec import add_features, add_tables, add_feature_tables




environment = Environment(loader=PackageLoader('features', 'templates'))

def get_jinja_map(feature_spec, table):
    data_spec = get_data_spec()
    m = {}
    m['dim_calendar'] = data_spec['calendar']
    m['eol_table'] = data_spec['eol_table']
    m['table'] = table
    m['feature'] = feature_spec
    return m


def get_query_template(feature, table):
    if feature['type'] == 'time_series_aggregate':
        template = environment.get_template("timeseries_growth.j2")
    elif feature['type'] == 'lookup' and table['type'] == 'dimension_type2':
        template = environment.get_template("type2_lookup.j2")
    elif feature['type'] == 'lookup' and table['type'] == 'dimension_type1':
        template = environment.get_template("type1_lookup.j2")
    
    elif feature['type'] == 'fact_aggregate':
        template = environment.get_template("fact_aggregations.j2")
 
    return template


def get_sql_for_feature(feature_name, eo_table=None):
    features = get_features()
    tables = get_tables()
    feature_spec = features[feature_name]
    table = tables[feature_spec['table']]
    m = get_jinja_map(feature_spec, table)
    if eo_table is not None:
        m['eol_table'] = eo_table

    template = get_query_template(feature_spec, table)
    sql = template.render(m)
    return sql


def test_sql():
    sql = []
    features = get_features()
    for feature_name, features_spec in features.items():
        sql.append(get_sql_for_feature(feature_name))
    return sql







