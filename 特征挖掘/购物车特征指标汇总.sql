-----------购物车特征指标汇总--------
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

-----------step 1 选取过去一年购物车行为数据----------------
drop table if exists bimining.member_churn_cart_info_1y_001;
create table bimining.member_churn_cart_info_1y_001 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 30), "-", "")
		and statis_date <= '${hivevar:statis_date}'
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;

drop table if exists bimining.member_churn_cart_info_1y_002;
create table bimining.member_churn_cart_info_1y_002 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 60), "-", "")
		and statis_date <=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 30), "-", "")
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;		
	
drop table if exists bimining.member_churn_cart_info_1y_003;
create table bimining.member_churn_cart_info_1y_003 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 90), "-", "")
		and statis_date <=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 60), "-", "")
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;	

drop table if exists bimining.member_churn_cart_info_1y_004;
create table bimining.member_churn_cart_info_1y_004 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 120), "-", "")
		and statis_date <=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 90), "-", "")
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;

drop table if exists bimining.member_churn_cart_info_1y_005;
create table bimining.member_churn_cart_info_1y_005 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 150), "-", "")
		and statis_date <=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 120), "-", "")
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;

drop table if exists bimining.member_churn_cart_info_1y_006;
create table bimining.member_churn_cart_info_1y_006 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 180), "-", "")
		and statis_date <=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 150), "-", "")
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;

drop table if exists bimining.member_churn_cart_info_1y_007;
create table bimining.member_churn_cart_info_1y_007 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 210), "-", "")
		and statis_date <=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 180), "-", "")
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;

drop table if exists bimining.member_churn_cart_info_1y_008;
create table bimining.member_churn_cart_info_1y_008 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 240), "-", "")
		and statis_date <=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 210), "-", "")
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;

drop table if exists bimining.member_churn_cart_info_1y_009;
create table bimining.member_churn_cart_info_1y_009 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 270), "-", "")
		and statis_date <=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 240), "-", "")
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;

drop table if exists bimining.member_churn_cart_info_1y_010;
create table bimining.member_churn_cart_info_1y_010 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 300), "-", "")
		and statis_date <=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 270), "-", "")
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;

drop table if exists bimining.member_churn_cart_info_1y_011;
create table bimining.member_churn_cart_info_1y_011 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date > regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 330), "-", "")
		and statis_date <=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 300), "-", "")
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;

drop table if exists bimining.member_churn_cart_info_1y_012;
create table bimining.member_churn_cart_info_1y_012 stored as orc as 
select t1.member_id,
		t2.dept_cd,
		t1.create_date
from (		
	select  regexp_replace(substring(create_time,1,10),'-','') as create_date,
         member_id,
         commodity_code as gds_cd
	from BI_SOR.TSOR_OR_CSC_CART_DETAILL_D t1
	where statis_date >= regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 365), "-", "")
		and statis_date <=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 330), "-", "")
	) t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_cd=t2.gds_cd
;

drop table if exists bimining.member_churn_cart_info_1y;
create table bimining.member_churn_cart_info_1y stored as orc as 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_001 
union all 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_002 
union all 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_003
union all 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_004
union all 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_005
union all 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_006
union all 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_007
union all 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_008
union all 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_009
union all 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_010
union all 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_011
union all 
select member_id,
		dept_cd,
		create_date
from bimining.member_churn_cart_info_1y_012;
-----------step 2 过去一年购物车行为统计----------------



insert overwrite table bimining.member_churn_cart_feature partition (statis_date='${hivevar:statis_date}')	
select ta.member_id,
        ta.dept_cd,
        ta.span_days,
        ta.tot_cart_num_1y,
        t1.tot_cart_num_1w,
        t2.tot_cart_num_2w,
        t3.tot_cart_num_3w,
        t4.tot_cart_num_1m,
        t5.tot_cart_num_2m,
        t6.tot_cart_num_3m,
        t7.tot_cart_num_6m
from (
    select  member_id,
			dept_cd,
			(datediff(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),
			from_unixtime(to_unix_timestamp(max(create_date),'yyyyMMdd'),'yyyy-MM-dd'))) as span_days	,
			count(1) as tot_cart_num_1y   
    from  bimining.member_churn_cart_info_1y
    where create_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),365),"-","")
			and create_date<='${hivevar:statis_date}'
    group by member_id,dept_cd
    ) ta
left join (
    select  member_id,
			dept_cd,
			count(1) as tot_cart_num_1w   
    from  bimining.member_churn_cart_info_1y
    where create_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),7),"-","")
		and create_date<='${hivevar:statis_date}'
    group by member_id,dept_cd 
    )t1
on ta.member_id=t1.member_id and ta.dept_cd=t1.dept_cd
left join (
    select  member_id,
			dept_cd,
			count(1) as tot_cart_num_2w   
    from  bimining.member_churn_cart_info_1y
    where create_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),14),"-","")
          and create_date<='${hivevar:statis_date}'
    group by member_id,dept_cd )t2
on ta.member_id=t2.member_id and ta.dept_cd=t2.dept_cd
left join (
    select  member_id,
            dept_cd,
            count(1) as tot_cart_num_3w   
    from  bimining.member_churn_cart_info_1y
    where create_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),21),"-","")
           and create_date<='${hivevar:statis_date}'
    group by member_id,dept_cd 
    )t3
on ta.member_id=t3.member_id and ta.dept_cd=t3.dept_cd
left join (
    select  member_id,
            dept_cd,
            count(1) as tot_cart_num_1m   
    from  bimining.member_churn_cart_info_1y
    where create_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),30),"-","")
          and create_date<='${hivevar:statis_date}'
    group by member_id,dept_cd 
    )t4
on ta.member_id=t4.member_id and ta.dept_cd=t4.dept_cd
left join (
    select  member_id,
			dept_cd,
			count(1) as tot_cart_num_2m   
    from  bimining.member_churn_cart_info_1y
    where create_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),60),"-","")
    and create_date<='${hivevar:statis_date}'
    group by member_id,dept_cd 
    )t5
on ta.member_id=t5.member_id and ta.dept_cd=t5.dept_cd
left join (
    select  member_id,
            dept_cd,
            count(1) as tot_cart_num_3m   
    from  bimining.member_churn_cart_info_1y
    where create_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),90),"-","")
          and create_date<='${hivevar:statis_date}'
    group by member_id,dept_cd 
    )t6
on ta.member_id=t6.member_id and ta.dept_cd=t6.dept_cd
left join (
    select  member_id,
            dept_cd,
            count(1) as tot_cart_num_6m   
    from  bimining.member_churn_cart_info_1y
    where create_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),180),"-","")
          and create_date<='${hivevar:statis_date}'
    group by member_id,dept_cd 
    )t7
on ta.member_id=t7.member_id and ta.dept_cd=t7.dept_cd;

