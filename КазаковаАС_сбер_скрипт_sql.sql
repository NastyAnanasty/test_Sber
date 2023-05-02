select Client_id, REPORT_DATE, TXN_AMOUNT*CCY_RATE_WITH_WEEKEND as sum_of_operations
from (select all_days, NVL(ccy_rate, lag(ccy_rate,1) ignore nulls over (order by all_days)) as ccy_rate_with_weekend
from
(select distinct (max_date - level + 1) as all_days
from(
select max(REPORT_DATE) as max_date, min(REPORT_DATE) as min_date
from Rates) connect by level <=(max_date - min_date +1)) t1
left join (select * from Rates where ccy_code = 840) t2
on t1.all_days = t2.REPORT_DATE
order by all_days ) t3
join Transactions
on t3.all_days = Transactions.Report_date 
/
/
select distinct NVL(credit_date.Client_id, debit_date.Client_id) as Client_id, NVL(credit_date.Report_date, debit_date.Report_date) as Report_date, NVL(CREDIT_AMOUNT, 0) as CREDIT_AMOUNT, NVL(DEBIT_AMOUNT, 0) as DEBIT_AMOUNT, Last_VSP
from 
(select Client_id, sum(Txn_amount) over(partition by Txn_type, to_char(Report_date,'MM.YYYY'), Client_id) as Debit_amount, to_char(Report_date,'MM.YYYY') as Report_date
from VSP_oper_data 
where Txn_type = 'debit') debit_date
full join 
(select Client_id, sum(Txn_amount) over(partition by Txn_type, to_char(Report_date,'MM.YYYY'), Client_id) as Credit_amount, to_char(Report_date,'MM.YYYY') as Report_date
from VSP_oper_data 
where Txn_type = 'credit') credit_date
on credit_date.Report_date = debit_date.Report_date and credit_date.Client_id = debit_date.Client_id
inner join 
(select distinct Client_id, to_char(Report_date,'MM.YYYY') as Report_date, VSP_NUMBER as Last_VSP
from( Select Client_id, VSP_Number, Report_date,month_year, day_month, to_date(CONCAT(max(day_month) over (partition by Client_id, month_year), REGEXP_REPLACE(month_year, '^\d\d','')), 'DD.MM.YYYY') as max_day_in_mohth
from ( Select Client_id, VSP_Number, Report_date,to_char(Report_date,'MM.YYYY') as month_year, to_char(Report_date,'DD.MM') as day_month
from VSP_oper_data))
where REPORT_DATE = MAX_DAY_IN_MOHTH) last_vsp_date
on credit_date.Report_date = last_vsp_date.Report_date and credit_date.Client_id=last_vsp_date.Client_id
or debit_date.Report_date = last_vsp_date.Report_date and debit_date.Client_id=last_vsp_date.Client_id
order by Client_id
/
/
select Client_id, Report_date,  Debit_each_client/Debit_all_clients as Ratio
from (Select distinct Client_id, 
sum(Txn_amount) over(partition by Client_id, to_char(Report_date,'MM.YYYY')) as Debit_each_client, to_char(Report_date,'MM.YYYY') as Report_date, 
sum(Txn_amount) over(partition by to_char(Report_date,'MM.YYYY')) as Debit_all_clients
from VSP_oper_data 
where Txn_type = 'debit')
/