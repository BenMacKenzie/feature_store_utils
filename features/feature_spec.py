from features.hyper_feature import get_hyper_feature
from dotenv import find_dotenv
import os
import yaml


queries = {}
features = {}
tables = {}
feature_tables = {}
data_spec = {}

catalog_name = None
schema_name = None
fq_schema_name = None



def get_features():
  return features

def get_tables():
  return tables

def get_data_spec():
  return data_spec

def get_feature_tables():
  return feature_tables

def get_queries():
  return queries


def get_feature_set_location():
    feature_yaml = os.path.join(os.path.dirname(find_dotenv()), 'features.yaml')
    return feature_yaml


def load(d):
    global data_spec 
    data_spec = d
    add_features(d)
    add_tables(d)
    add_feature_tables(d)

def load_data_spec(data_spec=None):
    if data_spec is not None:
        load(data_spec)
    else:
        with open(get_feature_set_location(), "r") as stream:
            try:
                data_spec = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
               print(exc)
        load(data_spec)
    
    return get_data_spec()
    


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
    if 'feature_store' not in data_spec:
        return
    for t in data_spec['feature_store']:
        feature_tables[t['name']] = t


def get_template_location():
    filepath = resource_filename('features', 'templates')
    return filepath



