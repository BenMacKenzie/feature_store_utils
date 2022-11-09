import os
from dotenv import find_dotenv
from pkg_resources import resource_filename


def get_template_location():
    filepath = resource_filename('features', 'templates')

    return filepath

def get_feature_set_location():
    feature_yaml = os.path.join(os.path.dirname(find_dotenv()), 'features.yaml')
    return feature_yaml


print(get_template_location())
