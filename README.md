# feature_store_utils

### options for use

1. clone this repo.  create features.yaml.  follow demo notebook.
2. create you own repo and install as package (currently in testpypi).  See https://github.com/BenMacKenzie/churn_model_demo as an example.  Note that you must create a .env file in folder which contains the features.yaml file 


### to build: 

note that you need a token to do this...probably I am the only one who can push to testpypi at the moment.

```
python3 -m build  
python3 -m twine upload --repository testpypi dist/*

```

 
