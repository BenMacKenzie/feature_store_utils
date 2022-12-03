from pyspark.sql import SparkSession
import pytest
from datetime import date, timedelta
import pandas as pd
from pathlib import Path
import yaml
from features.feature_generation import build_training_data_set,feature_tables, build_feature_table, register_dimension_table, tables 


@pytest.fixture
def spark() -> SparkSession:
    """
    Create a spark session. Unit tests don't have access to the spark global
    """
    return SparkSession.builder.getOrCreate()



calendar_sql = """create or replace table dim_calendar as 
select
  year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as dateInt,
  calendarDate,
  year(calendarDate) AS calendarYyear,
  date_format(calendarDate, 'MMMM') as calendarMonth,
  month(calendarDate) as MonthOfYear,
  date_format(calendarDate, 'EEEE') as calendarDay,
  dayofweek(calendarDate) AS dayOfWeek,
  case
    when weekday(calendarDate) < 5 then 'Y'
    else 'N'
  end as IsWeekDay,
  dayofmonth(calendarDate) as dayOfMonth,
  case
    when calendarDate = last_day(calendarDate) then 'Y'
    else 'N'
  end as isLastDayOfMonth,
  dayofyear(calendarDate) as dayOfYear,
  weekofyear(calendarDate) as weekOfYearIso,
  quarter(calendarDate) as quarterOfYear,
  to_date(dateadd(day, - dayofweek(calendarDate), calendarDate)) as lastCompleteWeek,
  to_date(last_day(dateadd(month, -1, calendarDate))) as lastCompleteMonth
 
from
  dates
  """

def test_growth(spark):
    # Databricks notebook source
    spark.sql('create schema if not exists feature_utils_test')
    spark.sql('use feature_utils_test')
    beginDate = '2000-01-01'
    endDate = '2050-12-31'
    spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate").createOrReplaceTempView('dates')
    dim_calendar=Path('tests/time_series/dim_calendar.sql').read_text()
    spark.sql(dim_calendar)


    dates = [date(2019, 1, 1) + timedelta(n) for n in range(265)]
    customers = [101, 102]

    data = [[c, dates[d], 100*pow(1.05,d)] for c in customers for d in range(0,20)]
    dbu_df = pd.DataFrame (data, columns = ['customer_id', 'date', 'dbu'])
    spark.createDataFrame(dbu_df).write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('dbu')

    spark.sql("create or replace table renewal_eol (customer_id int, renewal_date date, commit boolean);")
    spark.sql("insert into table renewal_eol values (101, \"2019-01-20\", true)")

   

    with open('tests/time_series/features.yaml', "r") as stream:
        data_spec = yaml.safe_load(stream)

    df = build_training_data_set(data_spec)
    pdf = df.toPandas()
    assert pdf.iloc[0]['6_day_geometric_growth_sql_dbu']==1.05
    assert pdf.iloc[0]['6_day_average_growth_sql_dbu']==1.05








