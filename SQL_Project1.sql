use sample_db;
select*
from credit;
-- SQL porfolio project.
-- download credit card transactions dataset from below link :
-- https://www.kaggle.com/datasets/thedevastator/analyzing-credit-card-spending-habits-in-india
-- import the dataset in sql server with table name : credit_card_transcations
-- change the column names to lower case before importing data to sql server.Also replace space within column names with underscore.
-- (alternatively you can use the dataset present in zip file)
-- while importing make sure to change the data types of columns. by defualt it shows everything as varchar.

-- write 4-6 queries to explore the dataset and put your findings 

-- solve below questions
-- 1- write a query to print top 5 cities with highest spends and their percentage contribution of total credit card spends 
SELECT city, sum(amount)as sum_amount,
count(*) * 100.0/ (select count(*) from credit)as total_percentage
from credit
group by city
order by sum(amount) desc
limit 5;
-- 2- write a query to print highest spend month and amount spent in that month for each card type
with cte as (
SELECT card_type, sum(amount)as sum_amount,datepart(year,transaction_date) yt ,datepart(month ,transaction_date) mt
from credit
group by card_type,datepart(year,transaction_date)  ,datepart(month,transaction_date) 
order by card_type,sum_amount desc
)
select*
from (select *, rank() over(partition by card_type order by sum_amount desc)as rn
from cte)
a where rn=1;

with cte as(
select card_type, sum(amount) as sum_amount,
timestampdiff(year,transaction_date) yr, timestampdiff(month,transaction_date)mt
from credit
group by card_type,timestampdiff(year,transaction_date),timestampdiff(month,transaction_date)
order by card_type,sum_amount desc
)
select*
from (select *, rank() over(partition by card_type order by sum_amount desc)as rn
 from cte)
 where rn=1;
 
-- 3- write a query to print the transaction details(all columns from the table) for each card type when
	-- it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)
with cte as(
select*,
sum(amount) over(partition by card_type order by transaction_date,transaction_id)as total_spend
from credit
order by card_type,total_spend)
select *
from (select *, rank() over(partition by card_type order by total_spend)as rn
from cte
where total_spend>=1000000)
a where rn=1;


-- 4- write a query to find city which had lowest percentage spend for gold card type
with cte as(
select  city , card_type,sum(amount)as amount,
sum(case when card_type ='gold' then amount end )as gold_amount
from credit
group by city, card_type)
select 
city,sum(gold_amount)*1.0/sum(amount)as gold_ratio
from cte
group by city
having count (gold_amount) >0 and sum(gold_amount)>0
order by gold_ratio
limit 1;

-- 5- write a query to print 3 columns:  city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel)
select distinct exp_type from credit;
with cte as(
select city, exp_type,sum(amount)as total_amount
from credit
group by city,exp_type)
select
city,max(case when rn_asc = 1 then exp_type end )as lowest_exp_type,
min(case when rn_desc = 1 then exp_type end )as highest_exp_type
from
(select*,
rank() over(partition by city order by total_amount desc)rn_desc,
rank() over(partition by city order by total_amount asc) rn_asc
from cte) A
group by city;


-- 6- write a query to find percentage contribution of spends by females for each expense type

select  exp_type,
sum(case when gender='f' then amount else 0 end)*1.0/sum(amount )as total_percentage
from credit
group by exp_type
order by total_percentage ;



-- 7- which card and expense type combination saw highest month over month growth in Jan-2014
with cte as(
select card_type,exp_type,datepart(year,transaction_date)yt,
datepart(month,transaction_date)mt,sum(amount)as total_spend
from credit
group by card_type,datepart(year,transaction_date),datepart(month,transaction_date),exp_type
)
select *, (total_spend-prev_month_spend) as mom_growth
from(
select*,
lag(total_spend,1)over(partition by cardtype,exp_type order by yt,mt)as prev_month_spend
from cte)A
where prev_month_spend is not null and yt=2014 and mt=1
order by mom_growth desc;

-- 8 during weekends which city has highest total spend to total no of transcations ratio .

select city,sum(amount)*1.0/count(1) as ratio
from credit
where datepart(weekday,transaction_date)in(1,7) And datename(weekday.transaction_date)in("saturday","sunday")
 group by city
 order by ratio desc
 limit 1;
 
-- 9- which city took least number of days to reach its 500th transaction after the first transaction in that city
with cte as(
select *,
row_number() over (partition by city order by transaction_date,transaction_id) as  rn
from credit
)
select city, 
datediff(day, min(transaction_date),max(transaction_date))as datediff
from cte
where rn=1 or rn=500
group by city
having count(1)=2
order by datediff
;

-- once you are done with this create a github repo to put that link in your resume. Some example github links:
-- https://github.com/ptyadana/SQL-Data-Analysis-and-Visualization-Projects/tree/master/Advanced%20SQL%20for%20Application%20Development
-- https://github.com/AlexTheAnalyst/PortfolioProjects/blob/main/COVID%20Portfolio%20Project%20-%20Data%20Exploration.sql