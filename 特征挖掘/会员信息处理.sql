-----------会员信息处理--------
set hive.merge.mapredfiles = true;
set mapreduce.input.fileinputformat.split.maxsize=536870912;
set mapreduce.input.fileinputformat.split.minsize=134217728;
set mapred.min.split.size.per.node=10000000;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.exec.reducers.bytes.per.reducer=50000000;
set hive.exec.reducers.max=150;
set hive.exec.compress.intermediate=true; 

--------------------------宝宝年龄处理-------------------
--source:bimining.child_rec_range_new_d
drop table if exists bimining.member_churn_child_age_001;
create table bimining.member_churn_child_age_001 stored as orc as 
select member_id,
		(current_left_age+current_right_age)/2 as child_age,
		reliability_score_age, --可靠度得分
		reliability_grade_age, --可靠度等级
		accur_grade_age, --精确度
		'${hiveconf:statis_date}' as statis_date
from bimining.mom_status_children_age_and_gender
where statis_date='${hiveconf:statis_date}'
and member_id<>'';

--------------------------会员信息-------------------
--bi_dm.tdm_rkb_mem_lable_score_ed 会员标签得分全量表 
--bimining.tdm_credit_mem_lable_info_em_out 会员征信等级表

drop table bimining.member_churn_mem_info_001;
create table bimining.member_churn_mem_info_001 stored as orc as
select t.member_id,t.gender,
		nvl(t.age_value,0) as age_value,
		t.cust_level_num,
		t.purchase_power,
		t.loyalty_level,
		t2.scor_label,
		'${hiveconf:statis_date}' as statis_date,
		from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_time 
from bi_dm.tdm_rkb_mem_lable_score_ed t
left join(
select t1.member_id,t1.scor_label,max(t1.comp_scor) from bimining.tdm_credit_mem_lable_info_em_out t1
group by t1.member_id,t1.scor_label) t2
on t.member_id=t2.member_id
;