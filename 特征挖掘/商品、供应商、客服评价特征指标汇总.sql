-----------商品\供应商\客服评价特征指标汇总--------
set hive.merge.mapredfiles = true;
set mapreduce.input.fileinputformat.split.maxsize=536870912;
set mapreduce.input.fileinputformat.split.minsize=134217728;
set mapred.min.split.size.per.node=10000000;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.exec.reducers.bytes.per.reducer=50000000;
set hive.exec.reducers.max=150;
set hive.exec.compress.intermediate=true; --开启中间结果压缩

-----------step 1 选取过去一年商品评价行为数据----------------
drop table if exists bimining.member_churn_gds_evaluate_001;
create table bimining.member_churn_gds_evaluate_001 stored as orc as
select member_id,
		statis_date,
		gds_eval_pnt --商品评价星级
		--,best_tp   ----精华标签
		--,eval_add_flag  --是否有追评
		--,share_flag    --是否有晒单
from bi_sor.tsor_svc_gds_eval_d
where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),365),"-","")
and statis_date<='${hivevar:statis_date}'
and member_id<>''; 




insert overwrite table bimining.member_churn_gds_evaluate_feature partition (statis_date='${hivevar:statis_date}')	
select t1.member_id,
		t1.gds_cmnt_1y,
		t2.gds_cmnt_6m as gds_cmnt_6m,
		t3.gds_cmnt_3m as gds_cmnt_3m,
		t4.gds_cmnt_1m as gds_cmnt_1m,
		t5.gds_cmnt_3w as gds_cmnt_3w,
		t6.gds_cmnt_2w as gds_cmnt_2w,
		t7.gds_cmnt_1w as gds_cmnt_1w
from (
	select member_id,
			sum(case when gds_eval_pnt=1 then 1 else 0 end) as gds_cmnt_1y
	from bimining.member_churn_gds_evaluate_001
	group by member_id
	) t1
left join (
	select member_id,
			sum(case when gds_eval_pnt=1 then 1 else 0 end) as gds_cmnt_6m
	from bimining.member_churn_gds_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),180),"-","")
	group by member_id
	) t2 on t1.member_id=t2.member_id
left join (
	select member_id,
			sum(case when gds_eval_pnt=1 then 1 else 0 end) as gds_cmnt_3m
	from bimining.member_churn_gds_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),90),"-","")
	group by member_id
	) t3 on t1.member_id=t3.member_id
left join (
	select member_id,
			sum(case when gds_eval_pnt=1 then 1 else 0 end) as gds_cmnt_1m
	from bimining.member_churn_gds_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),30),"-","")
	group by member_id
	) t4 on t1.member_id=t4.member_id
left join (
	select member_id,
			sum(case when gds_eval_pnt=1 then 1 else 0 end) as gds_cmnt_3w
	from bimining.member_churn_gds_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),21),"-","")
	group by member_id
	) t5 on t1.member_id=t5.member_id
left join (
	select member_id,
			sum(case when gds_eval_pnt=1 then 1 else 0 end) as gds_cmnt_2w
	from bimining.member_churn_gds_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),14),"-","")
	group by member_id
	) t6 on t1.member_id=t6.member_id
left join (
	select member_id,
			sum(case when gds_eval_pnt=1 then 1 else 0 end) as gds_cmnt_1w
	from bimining.member_churn_gds_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),7),"-","")
	group by member_id
	) t7 on t1.member_id=t7.member_id;
	
	
-----------step 2 选取过去一年供应商及服务评价行为数据----------------
drop table if exists bimining.member_churn_vendor_evaluate_001;
create table bimining.member_churn_vendor_evaluate_001 stored as orc as 
select member_id,
		att_eval_pnt,--服务态度星级
		lgs_eval_pnt,  --物流送货速度星级
		statis_date
from bi_sor.tsor_svc_vendor_eval_d 
where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),365),"-","")
and statis_date<='${hivevar:statis_date}'
and member_id<>'';




insert overwrite table bimining.member_churn_vendor_evaluate_feature partition (statis_date='${hivevar:statis_date}')	
select t1.member_id,
		t1.att_amnt_1y,
		t2.att_amnt_6m as att_amnt_6m,
		t3.att_amnt_3m as att_amnt_3m,
		t4.att_amnt_1m as att_amnt_1m,
		t5.att_amnt_3w as att_amnt_3w,
		t6.att_amnt_2w as att_amnt_2w,
		t7.att_amnt_1w as att_amnt_1w,
		t1.lgs_amnt_1y as lgs_amnt_1y,
		t2.lgs_amnt_6m as lgs_amnt_6m,
		t3.lgs_amnt_3m as lgs_amnt_3m,
		t4.lgs_amnt_1m as lgs_amnt_1m,
		t5.lgs_amnt_3w as lgs_amnt_3w,
		t6.lgs_amnt_2w as lgs_amnt_2w,
		t7.lgs_amnt_1w as lgs_amnt_1w
from (
	select member_id,
			sum(case when att_eval_pnt=1 then 1 else 0 end) as att_amnt_1y,
			sum(case when lgs_eval_pnt=1 then 1 else 0 end ) as lgs_amnt_1y
	from bimining.member_churn_vendor_evaluate_001
	group by member_id
	)t1
left join (
	select member_id,
			sum(case when att_eval_pnt=1 then 1 else 0 end) as att_amnt_6m,
			sum(case when lgs_eval_pnt=1 then 1 else 0 end ) as lgs_amnt_6m
	from bimining.member_churn_vendor_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),180),"-","")
	group by member_id
	) t2
on t1.member_id=t2.member_id
left join (
	select member_id,
			sum(case when att_eval_pnt=1 then 1 else 0 end) as att_amnt_3m,
			sum(case when lgs_eval_pnt=1 then 1 else 0 end ) as lgs_amnt_3m
	from bimining.member_churn_vendor_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),90),"-","")
	group by member_id
	) t3
on t1.member_id=t3.member_id
left join (
	select member_id,
			sum(case when att_eval_pnt=1 then 1 else 0 end) as att_amnt_1m,
			sum(case when lgs_eval_pnt=1 then 1 else 0 end ) as lgs_amnt_1m
	from bimining.member_churn_vendor_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),30),"-","")
	group by member_id
	) t4
on t1.member_id=t4.member_id
left join (
	select member_id,
			sum(case when att_eval_pnt=1 then 1 else 0 end) as att_amnt_3w,
			sum(case when lgs_eval_pnt=1 then 1 else 0 end ) as lgs_amnt_3w
	from bimining.member_churn_vendor_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),21),"-","")
	group by member_id
	) t5
on t1.member_id=t5.member_id
left join (
	select member_id,
			sum(case when att_eval_pnt=1 then 1 else 0 end) as att_amnt_2w,
			sum(case when lgs_eval_pnt=1 then 1 else 0 end ) as lgs_amnt_2w
	from bimining.member_churn_vendor_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),14),"-","")
	group by member_id
	) t6
on t1.member_id=t6.member_id
left join (
	select member_id,
			sum(case when att_eval_pnt=1 then 1 else 0 end) as att_amnt_1w,
			sum(case when lgs_eval_pnt=1 then 1 else 0 end ) as lgs_amnt_1w
	from bimining.member_churn_vendor_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),7),"-","")
	group by member_id
	) t7
on t1.member_id=t7.member_id;


-----------step 3 选取过去一年客服评价行为数据----------------
--source:bi_sor.tsor_crm_sn_chat_option_d option 3及以下代表不满意
--source:bi_sor.tsor_suc_task_info_d 客服工单表，在工单里的就被视为投诉

drop table if exists bimining.member_churn_survice_evaluate_001;
create table bimining.member_churn_survice_evaluate_001 stored as orc as 
select cust_num as member_id,
		regexp_replace(substring(create_time,0,10),"-","") as statis_date
from bi_sor.tsor_suc_task_info_d
where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),365),"-","")
and statis_date<='${hivevar:statis_date}'
and cust_num<>'';




insert overwrite table bimining.member_churn_survice_evaluate_feature partition (statis_date='${hivevar:statis_date}')	
select t1.member_id,
		t1.complaints_1y as complaints_1y,
		t2.complaints_6m as complaints_6m,
		t3.complaints_3m as complaints_3m,
		t4.complaints_1m as complaints_1m,
		t5.complaints_3w as complaints_3w,
		t6.complaints_2w as complaints_2w,
		t7.complaints_1w as complaints_1w
from (
	select member_id,
			count(distinct statis_date) as complaints_1y
	from bimining.member_churn_survice_evaluate_001
	group by member_id
	)t1
left join (
	select member_id,
			count(distinct statis_date) as complaints_6m
	from bimining.member_churn_survice_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),180),"-","")
	group by member_id
	) t2
on t1.member_id=t2.member_id
left join (
	select member_id,
			count(distinct statis_date) as complaints_3m
	from bimining.member_churn_survice_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),90),"-","")
	group by member_id
	) t3
on t1.member_id=t3.member_id
left join (
	select member_id,
			count(distinct statis_date) as complaints_1m
	from bimining.member_churn_survice_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),30),"-","")
	group by member_id
	) t4
on t1.member_id=t4.member_id
left join (
	select member_id,
			count(distinct statis_date) as complaints_3w
	from bimining.member_churn_survice_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),21),"-","")
	group by member_id
	) t5
on t1.member_id=t5.member_id
left join (
	select member_id,
			count(distinct statis_date) as complaints_2w
	from bimining.member_churn_survice_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),14),"-","")
	group by member_id
	) t6
on t1.member_id=t6.member_id
left join (
	select member_id,
			count(distinct statis_date) as complaints_1w
	from bimining.member_churn_survice_evaluate_001
	where statis_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),7),"-","")
	group by member_id
	) t7
on t1.member_id=t7.member_id;