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

--bimining.MEMBER_VISIT_DEPT_FEATURES_1W t7
--bimining.MEMBER_VISIT_DEPT_FEATURES_2W t6
--bimining.MEMBER_VISIT_DEPT_FEATURES_3W t5
--bimining.MEMBER_VISIT_DEPT_FEATURES_1m t4
--bimining.MEMBER_VISIT_DEPT_FEATURES_3m t3
--bimining.MEMBER_VISIT_DEPT_FEATURES_6m t2
--bimining.MEMBER_VISIT_DEPT_FEATURES_1Y t1
--set statis_date=20180131;

--合并特征
DROP TABLE

IF EXISTS bimining.member_churn_visit_feature_100;
    CREATE TABLE bimining.member_churn_visit_feature_100 stored AS orc AS

SELECT t1.member_id
    ,t1.dept_cd
    ,t1.visit_amnt_1Y
    ,t1.visit_days_1Y
    ,t1.avg_vist_1Y
    ,t1.avg_drtn_1Y
    ,t1.visit_grps_1Y
    ,t2.visit_amnt_6m
    ,t2.visit_days_6m
    ,t2.avg_vist_6m
    ,t2.avg_drtn_6m
    ,t2.visit_grps_6m
    ,t3.visit_amnt_3m
    ,t3.visit_days_3m
    ,t3.avg_vist_3m
    ,t3.avg_drtn_3m
    ,t3.visit_grps_3m
    ,t4.visit_amnt_1m
    ,t4.visit_days_1m
    ,t4.avg_vist_1m
    ,t4.avg_drtn_1m
    ,t4.visit_grps_1m
    ,t5.visit_amnt_3w
    ,t5.visit_days_3w
    ,t5.avg_vist_3w
    ,t5.avg_drtn_3w
    ,t5.visit_grps_3w
    ,t6.visit_amnt_2w
    ,t6.visit_days_2w
    ,t6.avg_vist_2w
    ,t6.avg_drtn_2w
    ,t6.visit_grps_2w
    ,t7.visit_amnt_1w
    ,t7.visit_days_1w
    ,t7.avg_vist_1w
    ,t7.avg_drtn_1w
    ,t7.visit_grps_1w
    ,'${hivevar:statis_date}' AS statis_date
FROM bimining.MEMBER_VISIT_DEPT_FEATURES_1Y t1
LEFT JOIN bimining.MEMBER_VISIT_DEPT_FEATURES_6m t2 ON t1.member_id = t2.member_id
    AND t1.dept_cd = t2.dept_cd
LEFT JOIN bimining.MEMBER_VISIT_DEPT_FEATURES_3m t3 ON t1.member_id = t3.member_id
    AND t1.dept_cd = t3.dept_cd
LEFT JOIN bimining.MEMBER_VISIT_DEPT_FEATURES_1m t4 ON t1.member_id = t4.member_id
    AND t1.dept_cd = t4.dept_cd
LEFT JOIN bimining.MEMBER_VISIT_DEPT_FEATURES_3W t5 ON t1.member_id = t5.member_id
    AND t1.dept_cd = t5.dept_cd
LEFT JOIN bimining.MEMBER_VISIT_DEPT_FEATURES_2W t6 ON t1.member_id = t6.member_id
    AND t1.dept_cd = t6.dept_cd
LEFT JOIN bimining.MEMBER_VISIT_DEPT_FEATURES_1W t7 ON t1.member_id = t7.member_id
    AND t1.dept_cd = t7.dept_cd;
	
--插入特征分区表
drop table bimining.member_churn_visit_feature;
create table bimining.member_churn_visit_feature
(member_id	string,
dept_cd	string,
visit_amnt_1w	bigint,
visit_days_1w	bigint,
avg_vist_1w	double,
avg_drtn_1w	double,
visit_grps_1w	bigint,
visit_amnt_2w	bigint,
visit_days_2w	bigint,
avg_vist_2w	double,
avg_drtn_2w	double,
visit_grps_2w	bigint,
visit_amnt_3w	bigint,
visit_days_3w	bigint,
avg_vist_3w	double,
avg_drtn_3w	double,
visit_grps_3w	bigint,
visit_amnt_1m	bigint,
visit_days_1m	bigint,
avg_vist_1m	double,
avg_drtn_1m	double,
visit_grps_1m	bigint,
visit_amnt_3m	bigint,
visit_days_3m	bigint,
avg_vist_3m	double,
avg_drtn_3m	double,
visit_grps_3m	bigint,
visit_amnt_6m	bigint,
visit_days_6m	bigint,
avg_vist_6m	double,
avg_drtn_6m	double,
visit_grps_6m	bigint,
visit_amnt_1y	bigint,
visit_days_1y	bigint,
avg_vist_1y	double,
avg_drtn_1y	double,
visit_grps_1y	bigint
)
partitioned by (statis_date string)
stored as orc;

INSERT overwrite TABLE bimining.member_churn_visit_feature PARTITION (statis_date='${hivevar:statis_date}')
SELECT member_id
    ,dept_cd
    ,nvl(visit_amnt_1w, 0)
    ,nvl(visit_days_1w, 0)
    ,nvl(avg_vist_1w, 0)
    ,nvl(avg_drtn_1w, 0)
    ,nvl(visit_grps_1w, 0)
    ,nvl(visit_amnt_2w, 0)
    ,nvl(visit_days_2w, 0)
    ,nvl(avg_vist_2w, 0)
    ,nvl(avg_drtn_2w, 0)
    ,nvl(visit_grps_2w, 0)
    ,nvl(visit_amnt_3w, 0)
    ,nvl(visit_days_3w, 0)
    ,nvl(avg_vist_3w, 0)
    ,nvl(avg_drtn_3w, 0)
    ,nvl(visit_grps_3w, 0)
    ,nvl(visit_amnt_1m, 0)
    ,nvl(visit_days_1m, 0)
    ,nvl(avg_vist_1m, 0)
    ,nvl(avg_drtn_1m, 0)
    ,nvl(visit_grps_1m, 0)
    ,nvl(visit_amnt_3m, 0)
    ,nvl(visit_days_3m, 0)
    ,nvl(avg_vist_3m, 0)
    ,nvl(avg_drtn_3m, 0)
    ,nvl(visit_grps_3m, 0)
    ,nvl(visit_amnt_6m, 0)
    ,nvl(visit_days_6m, 0)
    ,nvl(avg_vist_6m, 0)
    ,nvl(avg_drtn_6m, 0)
    ,nvl(visit_grps_6m, 0)
    ,visit_amnt_1y
    ,visit_days_1y
    ,avg_vist_1y
    ,avg_drtn_1y
    ,visit_grps_1y
FROM bimining.member_churn_visit_feature_100;

