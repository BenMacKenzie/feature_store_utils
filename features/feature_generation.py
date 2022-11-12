import yaml
from datetime import datetime
from jinja2 import Environment, PackageLoader
from pyspark.sql import SparkSession
from databricks.feature_store.client import FeatureStoreClient
import time
import os
from dotenv import find_dotenv
from pkg_resources import resource_filename
from features.hyper_feature import get_hyper_feature


queries = {}
features = {}
tables = {}
feature_tables = {}
data_spec = {}

catalog_name = None
schema_name = None
fq_schema_name = None


def add_features(data_spec):
    for f in data_spec['features']:
        if f['name'].endswith('*'):
            multi_features = get_hyper_feature(f)
            for mf in multi_features:
                features[mf['name']] = mf
        else:
            features[f['name']] = f


def add_tables(data_spec):
    for t in data_spec['tables']:
        tables[t['name']] = t


def add_feature_tables(data_spec):
    for t in data_spec['feature_store']:
        feature_tables[t['name']] = t


def get_jinja_map(feature_spec, table):
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
    elif feature['type'] == 'fact_aggregate':
        template = environment.get_template("fact_aggregations.j2")
    return template


def get_sql_for_feature(feature_name, eo_table=None):
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
    for feature_name, features_spec in features.items():
        sql.append(get_sql_for_feature(feature_name))
    return sql


def create_eo_table(entity_table, pk, start_date, end_date, grain, eo_table_name):
    calendar_table = data_spec['calendar']
    schema = data_spec['schema']

    spark.sql(f"use {schema}")

    if grain == 'day':
        date_col = 'calendarDay'
    elif grain == 'week':
        date_col = 'lastCompleteWeek'
    else:
        date_col = 'lastCompleteMonth'

    sql = f"with entities as (select distinct {pk} from {entity_table}), observation_dates as (select distinct {date_col} as observation_date from {calendar_table}) select {pk}, observation_date from observation_dates cross join entities \
    where observation_date >= dateadd({grain}, -1, to_date('{start_date}')) and observation_date <= to_date('{end_date}')"
    print(sql)

    spark.sql(sql).createOrReplaceTempView(eo_table_name)


def register_dimension_table(table):
    schema = data_spec['schema']
    spark.sql(f"use database {schema}")
    source_table_name = table['source_table_name']
    pk = table['pk']
    if table['type'] == 'dimension_type2':
        timestamp_key = table['row_effective_from']
        fs.register_table(delta_table=f"{schema}.{source_table_name}", primary_keys=pk, timestamp_keys=timestamp_key)
    else:
        fs.register_table(delta_table=f"{schema}.{source_table_name}", primary_keys=pk)


def build_feature_table(feature_table, drop_existing=False, update=False):

    schema = data_spec['schema']
    spark.sql(f"use database {schema}")

    table_name = feature_table['name']
    f_table_name = f"{schema}.{table_name}"

    entity_table = feature_table['entity_table']
    entity_name = feature_table['pk']
    start_date = feature_table['start_date']
    end_date = feature_table['end_date']
    grain = feature_table['grain']

    eo_table_name = f"eo_table_{int(time.time())}"

    eo_table = {
        "name": eo_table_name,
        "entity": {
            "name": entity_name
        },
        "observation_date": {
            "name": "observation_date"
        }
    }

    if update:
        start_date = datetime.today().strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')

    create_eo_table(entity_table, entity_name, start_date, end_date, grain, eo_table_name)

    df = spark.sql(f"select * from {eo_table_name}")

    if drop_existing:
        fs.drop_table(f_table_name)

    feature_list = []
    for feature_name in feature_table['features']:
        if feature_name.endswith('*'):
            prefix = feature_name[:-1]
            for key in features:
                if key.startswith(prefix):
                    feature_list.append(key)
        else:
            feature_list.append(feature_name)

    for feature_name in feature_list:
        select_clause = get_sql_for_feature(feature_name, eo_table)
        feature_df = spark.sql(select_clause)

        cond = [df[entity_name] == feature_df[entity_name], df['observation_date'] == feature_df['observation_date']]

        df = df.alias('a').join(feature_df.alias('b'), cond, 'left').select('a.*', f"b.{feature_name}")

        feature_spec = features[feature_name]
        if 'default_value' in feature_spec:
            df = df.na.fill(feature_spec['default_value'], [feature_name])

    if update:
        fs.write_table(f_table_name, df=df, mode='merge')
    else:
        fs.create_table(f_table_name, primary_keys=feature_table['pk'], timestamp_keys='observation_date', df=df)


def build_training_data_set():
    eol_table = data_spec['eol_table']
    eol_table_name = eol_table['name']
    entity_name = eol_table['entity']['name']
    observation_date_name = eol_table['observation_date']['name']

    schema = data_spec['schema']
    spark.sql(f"use database {schema}")

    df = spark.sql(f"select * from {eol_table_name}")

    for feature_name, features_spec in features.items():

        select_clause = get_sql_for_feature(feature_name)
        feature_df = spark.sql(select_clause)

        cond = [df[entity_name] == feature_df[entity_name],
                df[observation_date_name] == feature_df[observation_date_name]]

        df = df.alias('a').join(feature_df.alias('b'), cond, 'left').select('a.*', f"b.{feature_name}")

        feature_spec = features[feature_name]
        if 'default_value' in feature_spec:
            df = df.na.fill(feature_spec['default_value'], [feature_name])

    return df


def get_template_location():
    filepath = resource_filename('features', 'templates')
    return filepath


def get_feature_set_location():
    feature_yaml = os.path.join(os.path.dirname(find_dotenv()), 'features.yaml')
    return feature_yaml


with open(get_feature_set_location(), "r") as stream:
    try:
        data_spec = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

add_features(data_spec)
add_tables(data_spec)
add_feature_tables(data_spec)
environment = Environment(loader=PackageLoader('features', 'templates'))
spark = SparkSession.builder.getOrCreate()
fs = FeatureStoreClient()
