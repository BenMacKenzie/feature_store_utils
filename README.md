# feature store utils

A light-weight package that allows you express ML features in simple yaml, build a training data set and then write them to a feature store. 


### some general thoughts on building a training dataset
https://docs.google.com/presentation/d/1tVkrwCLVwFp8cZC7CmAHSNFhsJrcTdC20MlZfptkSBE/edit?usp=sharing


### options for use

1. clone this repo.  create features.yaml.  follow demo notebook. do not check back in.
2. install as a python package. See https://github.com/BenMacKenzie/churn_model_demo as an example.  



### Notes

1. Current version is experimental.  Not clear that Jinja is the right way to write parameterized SQL. Might be better to do in Python.
2. Current version is not optimized. Each feature is calculated individually, whereas if table, filters and time windows are identical, multiple aggregation features can be calculated simultaneously. 
3. I believe there are around a dozen standard feature types.  The most common have been implemented.  Note that views can fill in a lot of gaps if encountered.  missing:
  - type 1 lookup.
  - 1st order aggregations over time series (e.g., just treat it like a fact table)
  - 2nd order aggregations over time series e.g., max monthly job dbu over 6 month window.
  - time in state,  e.g., how long was a ticket open.  based on a type 2 table.
  - time to event in fact table, e.g., time since last call to customer support
  - scalar functions of two or more features, e.g, time in days between two date
  - num state changes over interval (rare)
  - functions of features (e.g., ratio of growth in job dbu to interactive dbu).  Arguably this is not needed for boosted trees. Might be useful for neural nets...but why use a nueral net on heterogeneous data? (actually this kind of thing can be good for model explainability)
4. Need to illustrate adding features from a related dimension table (using a foreign key...machinery is in place to do so.)
5. Current version illustrates creating a pipeline which uses the api.  But it would be nice just to generate the code and write it to a notebook so that the package is invisible in production (like bamboolib) 
9. The demo repo (https://github.com/BenMacKenzie/churn_model_demo) illustrates 'hyper-features' which are features with variable parameters. 
10. Connecting 'hyper-features' to feature store needs to be worked out.  Currently the option is to add all of them or specify individual version by their (generated) name
11. Fix feature store feature gen observation dates.  Align with grain of feature, e.g., if grain is monthly make sure feature store contains an observation on first of month.




 
