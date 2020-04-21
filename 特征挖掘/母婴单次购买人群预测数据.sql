-----------母婴单次购买人群预测数据--------
set hive.merge.mapredfiles = true;
set mapreduce.input.fileinputformat.split.maxsize=536870912;
set mapreduce.input.fileinputformat.split.minsize=134217728;
set mapred.min.split.size.per.node=10000000;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.exec.reducers.bytes.per.reducer=50000000;
set hive.exec.reducers.max=150;
set hive.exec.compress.intermediate=true; --开启中间结果压缩

--set statis_date=20170710;
------------------------------step 1 选取最近两年有购买行为的会员---------------------
drop table if exists bimining.member_churn_muying_once_predict_001;
create table bimining.member_churn_muying_once_predict_001 stored as orc as
select member_id,
		count(distinct pay_date)  as num_day --购买天数                                                                                           
from   bimining.member_churn_order_info                                                                                                
where  pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),730),"-","")   
       and pay_date<='${hivevar:statis_date}'
       and dept_cd=00019 --母婴事业部代码
group by member_id                                                                                                                               
having count(distinct pay_date)=1;  --购买天数1次
			
------------------------------step 2 选取最近7周有购买行为的会员---------------------	
drop table if exists bimining.member_churn_muying_once_predict_002;
create table bimining.member_churn_muying_once_predict_002 stored as orc as
select distinct member_id
from   bimining.member_churn_order_info
where  pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),79),"-","") 
       and pay_date<='${hivevar:statis_date}' 
       and dept_cd=00019;

drop table if exists bimining.member_churn_muying_once_predict_003;
create table bimining.member_churn_muying_once_predict_003 stored as orc as
select t1.member_id
from   bimining.member_churn_muying_once_predict_001 t1
inner join bimining.member_churn_muying_once_predict_002 t2
on t1.member_id=t2.member_id;	   
------------------------------step 3 会员特征合并---------------------	

drop table if exists bimining.member_churn_muying_once_predict_w;
create table bimining.member_churn_muying_once_predict_w stored as orc as
select t1.member_id,
		t2.gender,
		t2.age_value,
		case when t2.cust_level_num='161000000100' then '新人' 
			when t2.cust_level_num='161000000110' then 'V1'
			when t2.cust_level_num='161000000120' then 'V2'
			when t2.cust_level_num='161000000130' then 'V3'
			when t2.cust_level_num='161000000140' then 'V4' else 'other' end as cust_level_num,
		case when t2.purchase_power='-' then 'other' else t2.purchase_power end as purchase_power, 
		case when t2.loyalty_level='-' then 'other' else t2.loyalty_level end as loyalty_level,
		case when t2.scor_label is null then 'other' else t2.scor_label end as scor_label,
		(t3.current_left_age+t3.current_right_age)/2 as child_age,
		t3.reliability_score_age,
		case when t3.reliability_grade_age is null then 'other' else t3.reliability_grade_age end as reliability_grade_age,
		case when t3.accur_grade_age is null then 'other' else t3.accur_grade_age end as accur_grade_age,
		nvl(t4.visit_amnt_1y,0) as visit_amnt_1y,
		nvl(t4.visit_days_1y,0) as visit_days_1y,
		nvl(t4.avg_vist_1y,0) as avg_vist_1y,
		nvl(t4.avg_drtn_1y,0) as avg_drtn_1y,
		nvl(t4.visit_grps_1y,0) as visit_grps_1y,
		nvl(t4.visit_amnt_6m,0) as visit_amnt_6m,
		nvl(t4.visit_days_6m,0) as visit_days_6m,
		nvl(t4.avg_vist_6m,0) as avg_vist_6m,
		nvl(t4.avg_drtn_6m,0) as avg_drtn_6m,
		nvl(t4.visit_grps_6m,0) as visit_grps_6m,
		nvl(t4.visit_amnt_3m,0) as visit_amnt_3m,
		nvl(t4.visit_days_3m,0) as visit_days_3m,
		nvl(t4.avg_vist_3m,0) as avg_vist_3m,
		nvl(t4.avg_drtn_3m,0) as avg_drtn_3m,
		nvl(t4.visit_grps_3m,0) as visit_grps_3m,
		nvl(t4.visit_amnt_1m,0) as visit_amnt_1m,
		nvl(t4.visit_days_1m,0) as visit_days_1m,
		nvl(t4.avg_vist_1m,0) as avg_vist_1m,
		nvl(t4.avg_drtn_1m,0) as avg_drtn_1m,
		nvl(t4.visit_grps_1m,0) as visit_grps_1m,
		nvl(t4.visit_amnt_3w,0) as visit_amnt_3w,
		nvl(t4.visit_days_3w,0) as visit_days_3w,
		nvl(t4.avg_vist_3w,0) as avg_vist_3w,
		nvl(t4.avg_drtn_3w,0) as avg_drtn_3w,
		nvl(t4.visit_grps_3w,0) as visit_grps_3w,
		nvl(t4.visit_amnt_2w,0) as visit_amnt_2w,
		nvl(t4.visit_days_2w,0) as visit_days_2w,
		nvl(t4.avg_vist_2w,0) as avg_vist_2w,
		nvl(t4.avg_drtn_2w,0) as avg_drtn_2w,
		nvl(t4.visit_grps_2w,0) as visit_grps_2w,
		nvl(t4.visit_amnt_1w,0) as visit_amnt_1w,
		nvl(t4.visit_days_1w,0) as visit_days_1w,
		nvl(t4.avg_vist_1w,0) as avg_vist_1w,
		nvl(t4.avg_drtn_1w,0) as avg_drtn_1w,
		nvl(t4.visit_grps_1w,0) visit_grps_1w,
		nvl(t5.span_days,365) as recent_clct,
		nvl(t5.tot_clct_num_1y,0) as tot_clct_num_1y,
		nvl(t5.tot_clct_num_1w,0) as tot_clct_num_1w,
		nvl(t5.tot_clct_num_2w,0) as tot_clct_num_2w,
		nvl(t5.tot_clct_num_3w,0) as tot_clct_num_3w,
		nvl(t5.tot_clct_num_1m,0) as tot_clct_num_1m,
		nvl(t5.tot_clct_num_2m,0) as tot_clct_num_2m,
		nvl(t5.tot_clct_num_3m,0) as tot_clct_num_3m,
		nvl(t5.tot_clct_num_6m,0) as tot_clct_num_6m,
		nvl(t6.span_days,365) as recent_cart ,
		nvl(t6.tot_cart_num_1y,0) as tot_cart_num_1y,
		nvl(t6.tot_cart_num_1w,0) as tot_cart_num_1w,
		nvl(t6.tot_cart_num_2w,0) as tot_cart_num_2w,
		nvl(t6.tot_cart_num_3w,0) as tot_cart_num_3w,
		nvl(t6.tot_cart_num_1m,0) as tot_cart_num_1m,
		nvl(t6.tot_cart_num_2m,0) as tot_cart_num_2m,
		nvl(t6.tot_cart_num_3m,0) as tot_cart_num_3m,
		nvl(t6.tot_cart_num_6m,0) as tot_cart_num_6m,
		nvl(t7.recent_buy,365) as recent_buy,
		nvl(t7.buy_days_1y,0) as buy_days_1y,
		nvl(t7.time_span_buy_1y,0) as time_span_buy_1y,
		nvl(t7.cost_amnt_1y,0) as cost_amnt_1y,
		nvl(t7.order_amnt_1y,0) as order_amnt_1y,
		nvl(t7.buy_grps_1y,0) as buy_grps_1y,
		nvl(t7.buy_days_6m,0) as buy_days_6m,
		nvl(t7.time_span_buy_6m,0) as time_span_buy_6m,
		nvl(t7.cost_amnt_6m,0) as cost_amnt_6m,
		nvl(t7.order_amnt_6m,0) as order_amnt_6m,
		nvl(t7.buy_grps_6m,0) as buy_grps_6m,
		nvl(t7.buy_days_3m,0) as buy_days_3m,
		nvl(t7.time_span_buy_3m,0) as time_span_buy_3m,
		nvl(t7.cost_amnt_3m,0) as cost_amnt_3m,
		nvl(t7.order_amnt_3m,0) as order_amnt_3m,
		nvl(t7.buy_grps_3m,0) as buy_grps_3m,
		nvl(t7.buy_days_1m,0) as buy_days_1m,
		nvl(t7.time_span_buy_1m,0) as time_span_buy_1m,
		nvl(t7.cost_amnt_1m,0) as cost_amnt_1m,
		nvl(t7.order_amnt_1m,0) as order_amnt_1m,
		nvl(t7.buy_grps_1m,0) as buy_grps_1m,
		nvl(t7.buy_days_3w,0) as buy_days_3w,
		nvl(t7.time_span_buy_3w,0) as time_span_buy_3w,
		nvl(t7.cost_amnt_3w,0) as cost_amnt_3w,
		nvl(t7.order_amnt_3w,0) as order_amnt_3w,
		nvl(t7.buy_grps_3w,0) as buy_grps_3w,
		nvl(t7.buy_days_2w,0) as buy_days_2w,
		nvl(t7.time_span_buy_2w,0) as time_span_buy_2w,
		nvl(t7.cost_amnt_2w,0) as cost_amnt_2w,
		nvl(t7.order_amnt_2w,0) as order_amnt_2w,
		nvl(t7.buy_grps_2w,0) as buy_grps_2w,
		nvl(t7.buy_days_1w,0) as buy_days_1w,
		nvl(t7.time_span_buy_1w,0) as time_span_buy_1w,
		nvl(t7.cost_amnt_1w,0) as cost_amnt_1w,
		nvl(t7.order_amnt_1w,0) as order_amnt_1w,
		nvl(t7.buy_grps_1w,0) as buy_grps_1w,
		nvl(t7.return_amnt_1y,0) as return_amnt_1y,
		nvl(t7.return_amnt_6m,0) as return_amnt_6m,
		nvl(t7.return_amnt_3m,0) as return_amnt_3m,
		nvl(t7.return_amnt_1m,0) as return_amnt_1m,
		nvl(t7.return_amnt_3w,0) as return_amnt_3w,
		nvl(t7.return_amnt_2w,0) as return_amnt_2w,
		nvl(t7.return_amnt_1w,0) as return_amnt_1w,
		nvL(t8.complaints_1y,0) as complaints_1y,
		nvL(t8.complaints_6m,0) as complaints_6m,
		nvL(t8.complaints_3m,0) as complaints_3m,
		nvL(t8.complaints_1m,0) as complaints_1m,
		nvL(t8.complaints_3w,0) as complaints_3w,
		nvL(t8.complaints_2w,0) as complaints_2w,
		nvL(t8.complaints_1w,0) as complaints_1w,
		nvL(t9.att_amnt_1y,0) as att_amnt_1y,
		nvL(t9.att_amnt_6m,0) as att_amnt_6m,
		nvL(t9.att_amnt_3m,0) as att_amnt_3m,
		nvL(t9.att_amnt_1m,0) as att_amnt_1m,
		nvL(t9.att_amnt_3w,0) as att_amnt_3w,
		nvL(t9.att_amnt_2w,0) as att_amnt_2w,
		nvL(t9.att_amnt_1w,0) as att_amnt_1w,
		nvL(t9.lgs_amnt_1y,0) as lgs_amnt_1y,
		nvL(t9.lgs_amnt_6m,0) as lgs_amnt_6m,
		nvL(t9.lgs_amnt_3m,0) as lgs_amnt_3m,
		nvL(t9.lgs_amnt_1m,0) as lgs_amnt_1m,
		nvL(t9.lgs_amnt_3w,0) as lgs_amnt_3w,
		nvL(t9.lgs_amnt_2w,0) as lgs_amnt_2w,
		nvL(t9.lgs_amnt_1w,0) as lgs_amnt_1w,
		nvL(t10.gds_cmnt_1y,0) as gds_cmnt_1y,
		nvL(t10.gds_cmnt_6m,0) as gds_cmnt_6m,
		nvL(t10.gds_cmnt_3m,0) as gds_cmnt_3m,
		nvL(t10.gds_cmnt_1m,0) as gds_cmnt_1m,
		nvL(t10.gds_cmnt_3w,0) as gds_cmnt_3w,
		nvL(t10.gds_cmnt_2w,0) as gds_cmnt_2w,
		nvL(t10.gds_cmnt_1w,0) as gds_cmnt_1w,
		'${hivevar:statis_date}' as statis_date
from bimining.member_churn_muying_once_predict_003 t1
left join bimining.member_churn_mem_info_001 t2
on t1.member_id=t2.member_id
left join bimining.mom_status_children_age_and_gender t3
on t1.member_id=t3.member_id and t3.statis_date='${hivevar:statis_date}'
left join bimining.member_churn_visit_feature t4
on t1.member_id=t4.member_id and t4.statis_date='${hivevar:statis_date}' and t4.dept_cd=00019
left join bimining.member_churn_clct_feature t5
on t1.member_id=t5.cust_num and t5.statis_date='${hivevar:statis_date}' and t5.dept_cd=00019
left join bimining.member_churn_cart_feature t6
on t1.member_id=t6.member_id and t6.statis_date='${hivevar:statis_date}' and t6.dept_cd=00019
left join bimining.member_churn_pruchase_feature t7
on t1.member_id=t7.member_id and t7.statis_date='${hivevar:statis_date}' and t7.dept_cd=00019
left join bimining.member_churn_survice_evaluate_feature t8
on t1.member_id=t8.member_id and t8.statis_date='${hivevar:statis_date}'
left join bimining.member_churn_vendor_evaluate_feature t9
on t1.member_id=t9.member_id and t9.statis_date='${hivevar:statis_date}'
left join bimining.member_churn_gds_evaluate_feature t10
on t1.member_id=t10.member_id and t10.statis_date='${hivevar:statis_date}';