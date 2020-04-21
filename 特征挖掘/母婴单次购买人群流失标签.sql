-----------母婴单次购买人群流失标签--------
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

--set statis_date=20180126;
------------------------------step 1 选取最近两年有购买行为的会员---------------------
drop table if exists bimining.member_churn_muying_once_lose_label_001;
create table bimining.member_churn_muying_once_lose_label_001 stored as orc as
select member_id,
		count(distinct pay_date)  as num_day --购买天数                                                                                           
from   bimining.member_churn_order_info                                                                                                
where  pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),730),"-","")   
       and pay_date<='${hivevar:statis_date}'
       and dept_cd=00019 --母婴事业部代码
group by member_id                                                                                                                               
having count(distinct pay_date)=1;  --购买天数1次
			
------------------------------step 2 选取最近7周有购买行为的会员---------------------	
drop table if exists bimining.member_churn_muying_once_lose_label_002;
create table bimining.member_churn_muying_once_lose_label_002 stored as orc as
select distinct member_id
from   bimining.member_churn_order_info
where  pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),49),"-","") 
       and pay_date<='${hivevar:statis_date}' 
       and dept_cd=00019;

------------------------------step 3 针对未来7周是否购买，为会员打流失标签---------------------	
drop table if exists bimining.member_churn_muying_once_lose_label;
create table bimining.member_churn_muying_once_lose_label stored as orc as
select t1.member_id,
        case when t3.member_id is null then '1' else '0' end as whether_churn,
		'${hivevar:statis_date}' as statis_date
from bimining.member_churn_muying_once_lose_label_001 t1
left semi join bimining.member_churn_muying_once_lose_label_002 t2
on t1.member_id=t2.member_id
left join (
             select distinct( member_id)
             from bimining.member_churn_order_info 
             where length(member_id)>2
					and pay_date>='${hivevar:statis_date}'
                    and pay_date<=regexp_replace(date_add(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),49),"-","") 
                    and dept_cd=00019   
            ) t3
on t1.member_id=t3.member_id;	
	
