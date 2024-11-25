# Databricks notebook source
# MAGIC %md
# MAGIC #### use the next two cells to create and use database.  The database should correspond to the schema parameter in features.yaml

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog benmackenzie_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create database churn_model

# COMMAND ----------

# MAGIC %sql
# MAGIC use churn_model

# COMMAND ----------

from pyspark.sql.functions import explode, sequence, to_date

beginDate = '2000-01-01'
endDate = '2050-12-31'

(
  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
    .createOrReplaceTempView('dates')
)

# COMMAND ----------

# MAGIC %md
# MAGIC need to add start of week, end of week, start of month, end of month to facilitate time aggs

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

pip install Faker

# COMMAND ----------

from faker import Faker
from datetime import date, timedelta
import random
import pandas as pd

fake = Faker()
num_customers = 1000
start_date = date(2019, 1, 1)
num_days = 4 * 365 

dates = [start_date + timedelta(n) for n in range(num_days)]
customers = [fake.unique.random_int(min=100000, max=999999) for n in range(num_customers)]

data = [[c, d, fake.random_int(min=100, max=10000), fake.random_int(min=100, max=10000), fake.random_int(min=100, max=10000)] for c in customers for d in dates]
dbu_df = pd.DataFrame (data, columns = ['customer_id', 'date', 'interactive_dbu', 'job_dbu', 'sql_dbu'])

customer_tier=[]
for c in customers:
  d = random.randint(1, num_days-2)
  email = fake.ascii_company_email()
  customer_tier.append([c, email, random.choice(['standard', 'enterprise']), dates[0], dates[d]])
  customer_tier.append([c, email, random.choice(['standard', 'enterprise']), dates[d+1], date(2999,12,12)])
  

customers_df = pd.DataFrame(customer_tier, columns=['customer_id', 'email', 'tier', 'row_effective_from', 'row_expiration'])

renewal = [[c, date(2022, 1,1) + timedelta(random.randint(1,180)), random.randint(1,3), random.randint(1,100) > 20] for c in customers]
renewal_df = pd.DataFrame(renewal, columns=['customer_id', 'renewal_date', 'contract_length', 'commit'])


customer_support = []
for c in customers:
  d = random.randint(1, 100)
  while d < num_days:
    customer_support.append([c, dates[d], random.choice(['phone', 'email', 'slack'])])
    d += random.randint(1,100)

customer_support_df = pd.DataFrame(customer_support, columns=['customer_id', 'date', 'channel'])





# COMMAND ----------


spark.createDataFrame(dbu_df).write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('dbu')
spark.createDataFrame(customers_df).write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('customers')
spark.createDataFrame(renewal_df).write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('salesforce')
spark.createDataFrame(customer_support_df).write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('customer_support')



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers;

# COMMAND ----------


