------------------------------------------------------------------------------------------------------
--mobile banking data airtime transactions for 3 months (Jan - 10th may)

select customer_id, cast(date_created as date) Date_created, Amount
into stg.dbo.air_01
from tgt.dbo.derived_channels_billspayment
where transaction_type = 'Airtime' -- transaction_type = 'Data'
	and partition_key between 5845 and 5974 and transaction_status = 'successful'


--finding customers that fall into the daily bucket
with data_cte as (
select customer_id, 
	date_created, 
	lag(Date_created) over (partition by customer_id order by date_created) Previous_row_date, 
	dateadd(day, -1, date_created) Previousdate
,case when lag(date_created) over (partition by customer_id order by date_created) = dateadd(day, -1, date_created) then 1 else 0 end isConsecutive, amount
from dbo.air_01)

select customer_id, date_created, amount,
	case when isconsecutive = 1 then 1
	when lead(isconsecutive) over (partition by customer_id order by date_created) = 1 then 1 else 0 end as consecutive_days
	into stg.dbo.air_02
from data_cte


with cte as(
select *, DATEADD(dd, -(DATEPART(dw, date_created)-1), date_created) [WeekStart],
	DATEADD(dd, 7-(DATEPART(dw, date_created)), date_created) [WeekEnd],
	DATEPART(month, date_created) month_num	
from stg.dbo.air_02
where consecutive_days = 1)

select customer_id, date_created, WeekStart, WeekEnd, month_num, consecutive_days,
count(consecutive_days) over (partition by customer_id, weekstart order by date_created) no_of_days
	into stg.dbo.air_03
from cte
order by 1


with daily_cte as (
select distinct customer_id, WeekStart, WeekEnd, month_num,
	sum(consecutive_days) no_of_consecutive_txns
from air_03
where no_of_days >= 3
group by customer_id,WeekStart, WeekEnd, month_num, consecutive_days),

cte as (
select *, count(*) over (partition by customer_id, month_num order by weekstart) no_of_weeks
from daily_cte)

select distinct customer_id, WeekStart, WeekEnd, month_num, no_of_weeks,
	case when no_of_weeks >= 4 then 'Daily'
	else NULL end as freq_bucket
   into stg.dbo.air_04
from cte
where no_of_weeks >= 4 
order by 1


-- table with list of customers that fall under daily freq bucket
select distinct customer_id, freq_bucket
into stg.dbo.air_daily
from stg.dbo.air_04
where customer_id in (select customer_id from data_month)
group by customer_id, freq_bucket


--finding customers that fall into the weekly bucket
with data_cte as (
select customer_id, 
	date_created, 
	DATEADD(dd, -(DATEPART(dw, date_created)-1), date_created) [WeekStart],
	DATEADD(dd, 7-(DATEPART(dw, date_created)), date_created) [WeekEnd],
	DATEPART(month, date_created) month_num, 
	lag(DATEADD(dd, -(DATEPART(dw, date_created)-1), date_created)) over (partition by customer_id order by date_created) Previous_row_date,
	dateadd(week, -1, DATEADD(dd, -(DATEPART(dw, date_created)-1), date_created)) Previousweek,
	case when lag(DATEADD(dd, -(DATEPART(dw, date_created)-1), date_created)) over (partition by customer_id order by date_created) = dateadd(week, -1, DATEADD(dd, -(DATEPART(dw, date_created)-1), date_created)) then 1 else 0 end isConsecutive
from stg.dbo.air_01)

select customer_id, date_created, weekstart, weekend, month_num,
	case when isconsecutive = 1 then 1
	when lead(isconsecutive) over (partition by customer_id order by date_created) = 1 then 1 else 0 end as consecutive_weeks
	into stg.dbo.air_05
from data_cte


with weekly_cte as (
select distinct customer_id, WeekStart, WeekEnd, month_num, sum(consecutive_weeks) no_of_consecutive_txns
from stg.dbo.air_05
where consecutive_weeks = 1
group by WeekStart, WeekEnd, customer_id, month_num
)

select *, count(no_of_consecutive_txns) over (partition by customer_id, month_num order by weekstart)	no_of_weeks,
	sum(no_of_consecutive_txns) over (partition by customer_id, month_num order by weekstart) no_of_months_txn
	into stg.dbo.air_06
from weekly_cte


-- checking for people who have transacted weekly consecutively in all months
with cte1 as (
select customer_id, month_num, count(month_num) cnt
from air_06
group by customer_id, month_num),

cte as (
select *, count(*) over (partition by customer_id) cons_months
from cte1)

select distinct customer_id
into data_month
from cte
where cons_months >= 4
order by 1


select distinct customer_id, WeekStart, WeekEnd, month_num, no_of_weeks, no_of_months_txn,
	case when no_of_weeks >= 4 then 'Weekly'
	else NULL end as freq_bucket
into stg.dbo.air_07
from stg.dbo.air_06
where no_of_weeks >= 4
order by 1


-- table with list of customers that fall under weekly freq bucket
select distinct customer_id, freq_bucket
into stg.dbo.air_weekly
from stg.dbo.air_07
where customer_id in (select customer_id from data_month)
group by customer_id, freq_bucket


drop table if exists data_month
--finding customers that fall into the monthly bucket
with data_cte as (
select customer_id, 
	date_created, 
	cast(DATEADD(mm, DATEDIFF(mm, 0, date_created), 0) as date) MonthStart,
	--DATEADD(DD,-(DAY(date_created)), DATEADD(MM, 1, date_created)) AS LastMonth,
	DATEPART(month, date_created) month_num, 
	lag(cast(DATEADD(mm, DATEDIFF(mm, 0, date_created), 0) as date)) over (partition by customer_id order by date_created) Previous_row_month,
	cast(DATEADD(mm, DATEDIFF(mm, 0, date_created) - 1, 0) as date) Previousmnth,
	case when lag(cast(DATEADD(mm, DATEDIFF(mm, 0, date_created), 0) as date)) over (partition by customer_id order by date_created) = cast(DATEADD(mm, DATEDIFF(mm, 0, date_created) - 1, 0) as date) then 1 else 0 end isConsecutive
from stg.dbo.air_01)

select customer_id, date_created, MonthStart, month_num, 
	case when isconsecutive = 1 then 1
	when lead(isconsecutive) over (partition by customer_id order by date_created) = 1 then 1 else 0 end as consecutive_months
	into stg.dbo.air_08
from data_cte


with monthly_cte as (
select customer_id, MonthStart, month_num,
	sum(consecutive_months) cons_mnths,
	case when consecutive_months = 1 then 'Monthly'
	else NULL end as freq_bucket
from stg.dbo.air_08
where consecutive_months = 1
group by customer_id, MonthStart, month_num, consecutive_months
),

cte as (
select *, count(*) over (partition by customer_id) no_of_mnths
from monthly_cte
)

select *
into stg.dbo.air_09
from cte
where no_of_mnths >= 3


with mon_cte as (
select customer_id, Monthstart, month_num, freq_bucket
from stg.dbo.air_09
)

select distinct customer_id, freq_bucket
into stg.dbo.air_monthly
from mon_cte
group by customer_id, freq_bucket


-- adding to final airtime frequency bucket table
with final_cte as (
select distinct c.Customer_id, 
	dd.freq_bucket daily_freq_bucket, 
	wk.freq_bucket weekly_freq_bucket,
	mm.freq_bucket monthly_freq_bucket
from stg.dbo.air_01 c
left join stg.dbo.air_daily dd
on c.Customer_id = dd.customer_id
left join stg.dbo.air_weekly wk
on c.Customer_id = wk.customer_id
left join stg.dbo.air_monthly mm
on c.Customer_id = mm.customer_id)

select customer_id,
	case when daily_freq_bucket = 'Daily' then 'Daily'
		when weekly_freq_bucket = 'Weekly' then 'Weekly'
		when monthly_freq_bucket = 'Monthly' then 'Monthly'
		else 'None' end frequency_bucket
	into stg.dbo.air_final_freq_table
from final_cte


--FINAL TABLE
select *
from air_final_freq_table