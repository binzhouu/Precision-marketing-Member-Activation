--bimining.MID_MEMBER_VISIT_DEPT_l4_1D t 会员访问行为分区表
use bimining;
set hive.merge.mapredfiles = true;
set mapreduce.input.fileinputformat.split.maxsize=536870912;
set mapreduce.input.fileinputformat.split.minsize=134217728;
set mapred.min.split.size.per.node=10000000;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.exec.reducers.bytes.per.reducer=50000000;
set hive.exec.reducers.max=150;
set hive.exec.compress.intermediate=true; --开启中间结果压缩

--1W访问行为特征表
DROP TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_1W;

CREATE TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_1W stored AS orc AS

SELECT t.member_id
    ,t.dept_cd
    ,sum(visit_amnt) AS visit_amnt_1w                                                                                                                                                                                                                           
    ,count(DISTINCT t.statis_date) AS visit_days_1w
    ,sum(visit_gds) / 7 AS avg_vist_1w
    ,sum(visit_drtn) / sum(visit_gds) AS avg_drtn_1w
    ,count(DISTINCT l4_gds_group_cd) AS visit_grps_1w,
	'${hivevar:statis_date}' as statis_date
FROM bimining.DM_MEMBER_VISIT_DEPT_l4_D t
WHERE t.statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 7), "-", "")
    AND t.statis_date <= '${hivevar:statis_date}'
GROUP BY t.member_id
    ,t.dept_cd;




--2W访问行为特征表
DROP TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_2W;

CREATE TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_2W stored AS orc AS

SELECT t.member_id
	,t.dept_cd
	,sum(visit_amnt) AS visit_amnt_2w
	,count(DISTINCT t.statis_date) AS visit_days_2w
	,sum(visit_gds) / 14 AS avg_vist_2w
	,sum(visit_drtn) / sum(visit_gds) AS avg_drtn_2w
	,count(DISTINCT l4_gds_group_cd) AS visit_grps_2w
FROM bimining.DM_MEMBER_VISIT_DEPT_l4_D t
WHERE t.statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 14), "-", "")
	AND t.statis_date <= '${hivevar:statis_date}'
GROUP BY t.member_id
	,t.dept_cd;


--3W访问行为特征表
DROP TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_3W;

CREATE TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_3W stored AS orc AS

SELECT t.member_id
	,t.dept_cd
	,sum(visit_amnt) AS visit_amnt_3w
	,count(DISTINCT t.statis_date) AS visit_days_3w
	,sum(visit_gds) / 21 AS avg_vist_3w
	,sum(visit_drtn) / sum(visit_gds) AS avg_drtn_3w
	,count(DISTINCT l4_gds_group_cd) AS visit_grps_3w
FROM bimining.DM_MEMBER_VISIT_DEPT_l4_D t
WHERE t.statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 21), "-", "")
	AND t.statis_date <= '${hivevar:statis_date}'
GROUP BY t.member_id
	,t.dept_cd;


--1m访问行为特征表
DROP TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_1m;

CREATE TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_1m stored AS orc AS

SELECT t.member_id
	,t.dept_cd
	,sum(visit_amnt) AS visit_amnt_1m
	,count(DISTINCT t.statis_date) AS visit_days_1m
	,sum(visit_gds) / 30 AS avg_vist_1m
	,sum(visit_drtn) / sum(visit_gds) AS avg_drtn_1m
	,count(DISTINCT l4_gds_group_cd) AS visit_grps_1m
FROM bimining.DM_MEMBER_VISIT_DEPT_l4_D t
WHERE t.statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 30), "-", "")
	AND t.statis_date <= '${hivevar:statis_date}'
GROUP BY t.member_id
	,t.dept_cd;


--3m访问行为特征表
DROP TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_3m;

CREATE TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_3m stored AS orc AS

SELECT t.member_id
	,t.dept_cd
	,sum(visit_amnt) AS visit_amnt_3m
	,count(DISTINCT t.statis_date) AS visit_days_3m
	,sum(visit_gds) / 90 AS avg_vist_3m
	,sum(visit_drtn) / sum(visit_gds) AS avg_drtn_3m
	,count(DISTINCT l4_gds_group_cd) AS visit_grps_3m
FROM bimining.DM_MEMBER_VISIT_DEPT_l4_D t
WHERE t.statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 90), "-", "")
	AND t.statis_date <= '${hivevar:statis_date}'
GROUP BY t.member_id
	,t.dept_cd;


--6m访问行为特征表
DROP TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_6m;

CREATE TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_6m stored AS orc AS

SELECT t.member_id
	,t.dept_cd
	,sum(visit_amnt) AS visit_amnt_6m
	,count(DISTINCT t.statis_date) AS visit_days_6m
	,sum(visit_gds) / 180 AS avg_vist_6m
	,sum(visit_drtn) / sum(visit_gds) AS avg_drtn_6m
	,count(DISTINCT l4_gds_group_cd) AS visit_grps_6m
FROM bimining.DM_MEMBER_VISIT_DEPT_l4_D t
WHERE t.statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 180), "-", "")
	AND t.statis_date <= '${hivevar:statis_date}'
GROUP BY t.member_id
	,t.dept_cd;


--1Y访问行为特征表
DROP TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_1Y;

CREATE TABLE bimining.MEMBER_VISIT_DEPT_FEATURES_1Y stored AS orc AS

SELECT t.member_id
	,t.dept_cd
	,sum(visit_amnt) AS visit_amnt_1y
	,count(DISTINCT t.statis_date) AS visit_days_1y
	,sum(visit_gds) / 365 AS avg_vist_1y
	,sum(visit_drtn) / sum(visit_gds) AS avg_drtn_1y
	,count(DISTINCT l4_gds_group_cd) AS visit_grps_1y
FROM bimining.DM_MEMBER_VISIT_DEPT_l4_D t
WHERE t.statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 365), "-", "")
	AND t.statis_date <= '${hivevar:statis_date}'
GROUP BY t.member_id
	,t.dept_cd;
