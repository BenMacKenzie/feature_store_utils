create or replace table dim_calendar as 
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
