from features.feature_generation import build_training_data_set
df = build_training_data_set()
df.limit(10).show()