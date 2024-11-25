# Databricks notebook source
# MAGIC %sql
# MAGIC create schema benmackenzie_catalog.fs_content_streamer_demo_2

# COMMAND ----------

# MAGIC %sql
# MAGIC use benmackenzie_catalog.fs_content_streamer_demo_2

# COMMAND ----------

from pyspark.sql.functions import explode, sequence, to_date

beginDate = '2000-01-01'
endDate = '2050-12-31'

(
  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
    .createOrReplaceTempView('dates')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dim_calendar as
# MAGIC select
# MAGIC   year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as dateInt,
# MAGIC   calendarDate,
# MAGIC   year(calendarDate) AS calendarYyear,
# MAGIC   date_format(calendarDate, 'MMMM') as calendarMonth,
# MAGIC   month(calendarDate) as MonthOfYear,
# MAGIC   date_format(calendarDate, 'EEEE') as calendarDay,
# MAGIC   dayofweek(calendarDate) AS dayOfWeek,
# MAGIC   case
# MAGIC     when weekday(calendarDate) < 5 then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsWeekDay,
# MAGIC   dayofmonth(calendarDate) as dayOfMonth,
# MAGIC   case
# MAGIC     when calendarDate = last_day(calendarDate) then 'Y'
# MAGIC     else 'N'
# MAGIC   end as isLastDayOfMonth,
# MAGIC   dayofyear(calendarDate) as dayOfYear,
# MAGIC   weekofyear(calendarDate) as weekOfYearIso,
# MAGIC   quarter(calendarDate) as quarterOfYear,
# MAGIC   to_date(dateadd(day, - dayofweek(calendarDate), calendarDate)) as lastCompleteWeek,
# MAGIC   to_date(last_day(dateadd(month, -1, calendarDate))) as lastCompleteMonth
# MAGIC  
# MAGIC from
# MAGIC   dates
# MAGIC order by
# MAGIC   calendarDate

# COMMAND ----------

# MAGIC %pip install Faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from faker import Faker
from datetime import date, timedelta
import random
import pandas as pd
import numpy as np

fake = Faker()
num_customers = 1000
start_date = date(2019, 1, 1)
num_days = 5 * 365 

dates = [start_date + timedelta(n) for n in range(num_days)]
customers = [fake.unique.random_int(min=100000, max=999999) for n in range(num_customers)]


data = [[c, d, np.random.poisson(1), np.random.poisson(5), np.random.poisson(.2),
         np.random.poisson(.2), fake.random_int(min=0, max=200)] for c in customers for d in dates]


view_dbf = pd.DataFrame (data, columns = ['customer_id', 'date', 'num_logins', 'titles_viewed', 'titles_started', 'titles_completed', 'minutes_watched'])

customer_tier=[]
for c in customers:
  d1 = random.randint(1, int(num_days/2))
  d2 = random.randint(d1, num_days-2)
  email = fake.ascii_company_email()
  customer_tier.append([c, dates[d1], email, random.choice(['standard', 'premium']), random.choice(['EMEA', 'LATAM', 'APAC', 'NA']),dates[d1], dates[d2]])
  customer_tier.append([c, dates[d1], email, random.choice(['standard', 'premium']), random.choice(['EMEA', 'LATAM', 'APAC', 'NA']), dates[d2+1], date(2999,12,12)])
  

customers_df = pd.DataFrame(customer_tier, columns=['customer_id', 'initial_subscription_date', 'email', 'subscription', 'region', 'row_effective_from', 'row_expiration'])

renewal = [[c, date(2023, 1,1) + timedelta(random.randint(1,180)), random.choice(['month', 'annual']), random.randint(1,100) > 20] for c in customers]
renewal_df = pd.DataFrame(renewal, columns=['customer_id', 'renewal_date', 'contract_length', 'renew'])


customer_support = []
for c in customers:
  d = random.randint(1, 100)
  while d < num_days:
    customer_support.append([c, dates[d], random.choice(['phone', 'email'])])
    d += random.randint(1,100)

customer_support_df = pd.DataFrame(customer_support, columns=['customer_id', 'date', 'channel'])





# COMMAND ----------


spark.createDataFrame(view_dbf).write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('stream_history')
spark.createDataFrame(customers_df).write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('customers')
spark.createDataFrame(renewal_df).write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('subscription_history')
spark.createDataFrame(customer_support_df).write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('customer_support')



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers

# COMMAND ----------

pip install -r ../requirements.txt

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view renewal_eol as select customer_id, to_date(dateadd(month, -2, renewal_date)) as observation_date, renew from subscription_history where contract_length = 'annual'

# COMMAND ----------

from features.feature_generation import build_training_data_set
df = build_training_data_set()
display(df)

# COMMAND ----------

from features.feature_generation import build_feature_table
build_feature_table('customer_service_calls')

# COMMAND ----------

from features.feature_generation import build_feature_table
build_feature_table('viewing_aggregate')

# COMMAND ----------

# MAGIC %sql
# MAGIC --ALTER TABLE customers ALTER COLUMN customer_id SET NOT NULL;
# MAGIC --ALTER TABLE customers ALTER COLUMN row_effective_from SET NOT NULL;
# MAGIC --ALTER TABLE customers ADD CONSTRAINT pk_customers PRIMARY KEY(customer_id, row_effective_from TIMESERIES);
# MAGIC
# MAGIC --ALTER TABLE viewing_aggregate ALTER COLUMN customer_id SET NOT NULL;
# MAGIC --ALTER TABLE viewing_aggregate ALTER COLUMN calendarDate SET NOT NULL;
# MAGIC --ALTER TABLE viewing_aggregate ADD CONSTRAINT pk_customers_va PRIMARY KEY(customer_id, calendarDate TIMESERIES);
# MAGIC
# MAGIC --ALTER TABLE customer_service_calls_aggregate ALTER COLUMN customer_id SET NOT NULL;
# MAGIC --ALTER TABLE customer_service_calls_aggregate ALTER COLUMN calendarDate SET NOT NULL;
# MAGIC
# MAGIC --ALTER TABLE customer_service_calls_aggregate ADD CONSTRAINT pk_customer_support PRIMARY KEY(customer_id, calendarDate TIMESERIES);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from benmackenzie_catalog.fs_content_streamer_demo.viewing_aggregate
