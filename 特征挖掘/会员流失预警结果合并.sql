------------会员流失预警结果合并----------------------
use bimining;
set hive.merge.mapredfiles = true;
set mapreduce.input.fileinputformat.split.maxsize=536870912;
set mapreduce.input.fileinputformat.split.minsize=134217728;
set mapred.min.split.size.per.node=10000000;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.exec.reducers.bytes.per.reducer=50000000;
set hive.exec.reducers.max=150;
set hive.exec.compress.intermediate=true; 

truncate table member_churn_predictions_w_01;

--------------母婴一次结果插入------------------
insert into table bimining.member_churn_predictions_w_01
select member_id,
		"购买一次" as member_type,
		"母婴事业部" as dept_nm,
		"00019" as dept_cd,
		case when prediction=1.0 then prob else (1-prob) end as lose_rate
from bimining.member_churn_predict_muying_once;


--------------母婴二次结果插入------------------
insert into table bimining.member_churn_predictions_w_01
select member_id,
		"购买二次" as member_type,
		"母婴事业部" as dept_nm,
		"00019" as dept_cd,
		case when prediction=1.0 then prob else (1-prob) end as lose_rate
from bimining.member_churn_predict_muying_twice;

--------------超市结果插入------------------
insert into table bimining.member_churn_predictions_w_01
select member_id,
		"-" as member_type,
		"超市事业部" as dept_nm,
		"-" as dept_cd,
		case when prediction=1.0 then prob else (1-prob) end as lose_rate
from bimining.member_churn_predict_mkt;


--------------平台结果插入------------------
insert into table bimining.member_churn_predictions_w_01
select member_id,
		"-" as member_type,
		"易购平台" as dept_nm,
		"-" as dept_cd,
		case when prediction=1.0 then prob else (1-prob) end as lose_rate
from bimining.member_churn_predict_yigou;

--------------------最终结果表------------------
insert overwrite table bimining.member_churn_predictions_w partition (statis_date='${hivevar:statis_date}')
select member_id,
		member_type,
		dept_nm,
		dept_cd,
		max(lose_rate) as lose_rate
from member_churn_predictions_w_01
group by member_id,member_type,dept_nm,dept_cd;