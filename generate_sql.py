from features.feature_spec import load_data_spec
from features.sql_gen import test_sql

def main():
   load_data_spec()
   sql = test_sql()
   for q in sql:
      print(q)

if __name__ == "__main__":
    main()