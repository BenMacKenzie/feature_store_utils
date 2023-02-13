import itertools

def flatten(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if type(v) is dict:
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def unflatten(dictionary):
    resultDict = dict()
    for key, value in dictionary.items():
        parts = key.split(".")
        d = resultDict
        for part in parts[:-1]:
            if part not in d:
                d[part] = dict()
            d = d[part]
        d[parts[-1]] = value
    return resultDict

def get_hyper_feature(feature):

    feature_prefix = feature["name"][:-1]

    flat = flatten(feature)
    variable_columns = {}

    for k, v in flat.items():
        if type(v) is list:
            variable_columns[k] = v

    column_space = list(itertools.product(*list(variable_columns.values())))

    features = []
    for params in column_space:
        f = flat.copy()
        name = feature_prefix
        for i, item in enumerate(params):
            key = list(variable_columns.keys())[i]
            f[key] = params[i]
            col = key.split('.')[-1]
            name += f"_{col}_{params[i]}"
        f['name'] = name
        features.append(unflatten(f))

    return features
