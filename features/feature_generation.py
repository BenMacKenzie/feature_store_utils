from pyspark.sql import SparkSession
from databricks.feature_store.client import FeatureStoreClient
import time
import datetime
from features.sql_gen import get_sql_for_feature
from pyspark.sql import SparkSession
from databricks.feature_store.client import FeatureStoreClient
from features.feature_spec import load_data_spec, get_data_spec, get_features, get_feature_tables


def create_eo_table(entity_table, pk, start_date, end_date, grain, eo_table_name):
    spark = SparkSession.builder.getOrCreate()
    data_spec=get_data_spec()
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
    

    spark.sql(sql).createOrReplaceTempView(eo_table_name)



def register_dimension_table(table):
    spark = SparkSession.builder.getOrCreate()
    fs = FeatureStoreClient()
    data_spec=get_data_spec()
    schema = data_spec['schema']
    spark.sql(f"use database {schema}")
    source_table_name = table['source_table_name']
    pk = table['pk']
    if table['type'] == 'dimension_type2':
        timestamp_key = table['row_effective_from']
        fs.register_table(delta_table=f"{schema}.{source_table_name}", primary_keys=pk, timestamp_keys=timestamp_key)
    else:
        fs.register_table(delta_table=f"{schema}.{source_table_name}", primary_keys=pk)


def update_feature_table(feature_table):
    build_feature_table(feature_table, drop_existing=False, update=True)



def build_feature_table(feature_table_name, drop_existing=False, update=False):
    spark = SparkSession.builder.getOrCreate()
    data_spec = load_data_spec()
    feature_tables = get_feature_tables()
    feature_table = feature_tables[feature_table_name]
    features = get_features()
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
    fs = FeatureStoreClient()

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


def update_feature_table(feature_table):
    build_feature_table(feature_table, drop_existing=False, update=True)


def build_training_data_set(d=None, spark=None):
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
        
    data_spec = load_data_spec(d)
    features = get_features()
    
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


def register_dimension_table(table):
    fs = FeatureStoreClient()
    spark = SparkSession.builder.getOrCreate()
    data_spec=get_data_spec()
    schema = data_spec['schema']
    spark.sql(f"use database {schema}")
    source_table_name = table['source_table_name']
    pk = table['pk']
    if table['type'] == 'dimension_type2':
        timestamp_key = table['row_effective_from']
        fs.register_table(delta_table=f"{schema}.{source_table_name}", primary_keys=pk, timestamp_keys=timestamp_key)
    else:
        fs.register_table(delta_table=f"{schema}.{source_table_name}", primary_keys=pk)



#spark = SparkSession.builder.getOrCreate()
#fs = FeatureStoreClient()
