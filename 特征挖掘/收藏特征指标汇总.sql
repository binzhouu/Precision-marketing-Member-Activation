-----------收藏特征指标汇总--------
set hive.merge.mapredfiles = true;
set mapreduce.input.fileinputformat.split.maxsize=536870912;
set mapreduce.input.fileinputformat.split.minsize=134217728;
set mapred.min.split.size.per.node=10000000;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.exec.reducers.bytes.per.reducer=50000000;
set hive.exec.reducers.max=150;
set hive.exec.compress.intermediate=true; --开启中间结果压缩

-----------step 1 选取过去一年收藏行为数据----------------
drop table if exists bimining.member_churn_clct_info_001;
create table bimining.member_churn_clct_info_001 stored as orc as
select t1.cust_num,
		t1.updt_date,
		t2.dept_cd	
from BI_SOR.TSOR_PTY_PD_CLCT_INFO t1
inner join  bi_sor.tsor_pub_gds_info t2
on  t1.gds_id=t2.gds_cd
where  t1.updt_date >= regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 365), "-", "")
		and t1.updt_date <= '${hivevar:statis_date}';
		
-----------step 2 过去一年收藏行为统计----------------



insert overwrite table bimining.member_churn_clct_feature partition (statis_date='${hivevar:statis_date}')	
select ta.cust_num,
        ta.dept_cd,
        ta.span_days,
        ta. tot_clct_num_1y ,
        t1.tot_clct_num_1w as tot_clct_num_1w,
        t2.tot_clct_num_2w as tot_clct_num_2w,
        t3.tot_clct_num_3w as tot_clct_num_3w,
        t4.tot_clct_num_1m as tot_clct_num_1m,
        t5.tot_clct_num_2m as tot_clct_num_2m,
        t6.tot_clct_num_3m as tot_clct_num_3m,
        t7.tot_clct_num_6m as tot_clct_num_6m
from (
        select  cust_num,
                 dept_cd,
                 (datediff(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),
	                               from_unixtime(to_unix_timestamp(max(updt_date),'yyyyMMdd'),'yyyy-MM-dd'))) as span_days ,
                          count(1) as tot_clct_num_1y        
                  from  bimining.member_churn_clct_info_001
                  where updt_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),365),"-","")
                         and updt_date<='${hivevar:statis_date}'
                  group by cust_num,dept_cd
              ) ta
left join( 
                  select  cust_num,
                           dept_cd,
                           count(1) as tot_clct_num_1w  
                   from  bimining.member_churn_clct_info_001       
                   where updt_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),7),"-","")
                          and updt_date<='${hivevar:statis_date}'        
                   group by cust_num,dept_cd 
                ) t1
on ta.cust_num=t1.cust_num and ta.dept_cd=t1.dept_cd
left join (
                  select  cust_num,                  
                           dept_cd,                                                                                                                                 
                           count(1) as tot_clct_num_2w                                                                                                              
                  from  bimining.member_churn_clct_info_001                                                                                                   
                  where updt_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),14),"-","")        
                         and updt_date<='${hivevar:statis_date}'                                                                                                      
                  group by cust_num,dept_cd 
                 )  t2  
on ta.cust_num=t2.cust_num and ta.dept_cd=t2.dept_cd
left join (
                  select  cust_num,                  
                           dept_cd,                                                                                                                                 
                           count(1) as tot_clct_num_3w                                                                                                              
                  from  bimining.member_churn_clct_info_001                                                                                                   
                  where updt_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),21),"-","")        
                         and updt_date<='${hivevar:statis_date}'                                                                                                      
                  group by cust_num,dept_cd 
                 )  t3
on ta.cust_num=t3.cust_num and ta.dept_cd=t3.dept_cd
left join (
                  select  cust_num,                  
                           dept_cd,                                                                                                                                 
                           count(1) as tot_clct_num_1m                                                                                                              
                  from  bimining.member_churn_clct_info_001                                                                                                   
                  where updt_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),30),"-","")        
                         and updt_date<='${hivevar:statis_date}'                                                                                                      
                  group by cust_num,dept_cd 
                 )  t4
on ta.cust_num=t4.cust_num and ta.dept_cd=t4.dept_cd
left join  (
                  select  cust_num,                  
                           dept_cd,                                                                                                                                 
                           count(1) as tot_clct_num_2m                                                                                                              
                  from  bimining.member_churn_clct_info_001                                                                                                   
                  where updt_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),60),"-","")        
                         and updt_date<='${hivevar:statis_date}'                                                                                                      
                  group by cust_num,dept_cd 
                 )  t5
on ta.cust_num=t5.cust_num and ta.dept_cd=t5.dept_cd
left join (
                  select  cust_num,                  
                           dept_cd,                                                                                                                                 
                           count(1) as tot_clct_num_3m                                                                                                              
                  from  bimining.member_churn_clct_info_001                                                                                                   
                  where updt_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),90),"-","")        
                         and updt_date<='${hivevar:statis_date}'                                                                                                      
                  group by cust_num,dept_cd 
                 )  t6
on ta.cust_num=t6.cust_num and ta.dept_cd=t6.dept_cd
left join  (
                  select  cust_num,                  
                           dept_cd,                                                                                                                                 
                           count(1) as tot_clct_num_6m                                                                                                              
                  from  bimining.member_churn_clct_info_001                                                                                                   
                  where updt_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyyMMdd'),'yyyy-MM-dd'),180),"-","")        
                         and updt_date<='${hivevar:statis_date}'                                                                                                      
                  group by cust_num,dept_cd 
                 )  t7
on ta.cust_num=t7.cust_num and ta.dept_cd=t7.dept_cd;