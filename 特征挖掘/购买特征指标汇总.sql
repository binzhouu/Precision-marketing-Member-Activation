-----------购买特征指标汇总--------
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

-----------step 1 选取过去2年购买特征----------------
drop table if exists bimining.member_churn_order_info;
CREATE TABLE bimining.member_churn_order_info stored as orc as 
select ta.member_id,
        ta.pay_date,
        ta.pay_amnt,
        ta.order_id,
        ta.gds_cd,
        ta.catgroup_id,
        ta.bill_type,
        tb.dept_cd
from   BI_SOR.TSOR_OR_OMS_ORDER_DETAIL_D ta
join   BI_SOR.TSOR_CA_GDS_INF_TD tb
on     ta.gds_cd=tb.gds_cd    
where statis_date=pay_date
       and statis_date >= regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}', 'yyyyMMdd'), 'yyyy-MM-dd'), 730), "-", "")
		and statis_date <= '${hivevar:statis_date}'
       and chnl_cd in (10,20,50)
       and ta.member_id!='';
	   
-----------step 2 过去一年购买特征汇总----------------
drop table if exists bimining.member_churn_order_info_001;
CREATE TABLE bimining.member_churn_order_info_001 stored as orc as 
select member_id,
       dept_cd,             
       datediff(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),from_unixtime(to_unix_timestamp(max(pay_date),'yyyyMMdd'),'yyyy-MM-dd')) as recent_buy,
       count(distinct pay_date) as buy_days_1y,
       round((datediff(from_unixtime(to_unix_timestamp(max(pay_date),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(to_unix_timestamp(min(pay_date),'yyyyMMdd'),'yyyy-MM-dd')))/365,2) as time_span_buy_1y,
       round(sum(pay_amnt),2) as cost_amnt_1y,
       count(distinct order_id) as order_amnt_1y,
       count(distinct catgroup_id) as buy_grps_1y
from  bimining.member_churn_order_info 
where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),365),"-","") 
      and pay_date<='${hivevar:statis_date}'
      and bill_type=1     
GROUP BY member_id,dept_cd; 

-----------step 3 过去6个月购买特征汇总----------------  
drop table if exists bimining.member_churn_order_info_002;
CREATE TABLE bimining.member_churn_order_info_002 stored as orc as 
select  member_id,
        dept_cd,                      
        count(distinct pay_date) as buy_days_6m,
        round((datediff(from_unixtime(to_unix_timestamp(max(pay_date),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(to_unix_timestamp(min(pay_date),'yyyyMMdd'),'yyyy-MM-dd')))/180,2) as time_span_buy_6m,
        round(sum(pay_amnt),2) as cost_amnt_6m,
        count(distinct order_id) as order_amnt_6m,
        count(distinct catgroup_id) as buy_grps_6m
from bimining.member_churn_order_info 
where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),180),"-","") 
      and pay_date<='${hivevar:statis_date}'
      and bill_type=1     
GROUP BY member_id,dept_cd;

-----------step 4 过去3个月购买特征汇总----------------  
drop table if exists bimining.member_churn_order_info_003;
CREATE TABLE bimining.member_churn_order_info_003 stored as orc as 
select  member_id,
        dept_cd,                      
        count(distinct pay_date) as buy_days_3m,
        round((datediff(from_unixtime(to_unix_timestamp(max(pay_date),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(to_unix_timestamp(min(pay_date),'yyyyMMdd'),'yyyy-MM-dd')))/90,2) as time_span_buy_3m,
        round(sum(pay_amnt),2) as cost_amnt_3m,
        count(distinct order_id) as order_amnt_3m,
        count(distinct catgroup_id) as buy_grps_3m
from bimining.member_churn_order_info 
where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),90),"-","") 
      and pay_date<='${hivevar:statis_date}'
      and bill_type=1     
GROUP BY member_id,dept_cd;    

-----------step 5 过去1个月购买特征汇总----------------  
drop table if exists bimining.member_churn_order_info_004;
CREATE TABLE bimining.member_churn_order_info_004 stored as orc as 
select  member_id,
        dept_cd,                      
        count(distinct pay_date) as buy_days_1m,
        round((datediff(from_unixtime(to_unix_timestamp(max(pay_date),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(to_unix_timestamp(min(pay_date),'yyyyMMdd'),'yyyy-MM-dd')))/30,2) as time_span_buy_1m,
        round(sum(pay_amnt),2) as cost_amnt_1m,
        count(distinct order_id) as order_amnt_1m,
        count(distinct catgroup_id) as buy_grps_1m
from     bimining.member_churn_order_info 
where    pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),30),"-","") 
       and pay_date<='${hivevar:statis_date}'
       and bill_type=1     
GROUP BY member_id,dept_cd;


-----------step 6 过去3周购买特征汇总----------------  
drop table if exists bimining.member_churn_order_info_005;
CREATE TABLE bimining.member_churn_order_info_005 stored as orc as 
select  member_id,
        dept_cd,                      
        count(distinct pay_date) as buy_days_3w,
        round((datediff(from_unixtime(to_unix_timestamp(max(pay_date),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(to_unix_timestamp(min(pay_date),'yyyyMMdd'),'yyyy-MM-dd')))/21,2) as time_span_buy_3w,
        round(sum(pay_amnt),2) as cost_amnt_3w,
        count(distinct order_id) as order_amnt_3w,
        count(distinct catgroup_id) as buy_grps_3w
from bimining.member_churn_order_info 
where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),21),"-","") 
      and pay_date<='${hivevar:statis_date}'
      and bill_type=1     
GROUP BY member_id,dept_cd;

-----------step 7 过去2周购买特征汇总----------------  
drop table if exists bimining.member_churn_order_info_006;
CREATE TABLE bimining.member_churn_order_info_006 stored as orc as 
select  member_id,
        dept_cd,                      
        count(distinct pay_date) as buy_days_2w,
        round((datediff(from_unixtime(to_unix_timestamp(max(pay_date),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(to_unix_timestamp(min(pay_date),'yyyyMMdd'),'yyyy-MM-dd')))/14,2) as time_span_buy_2w,
        round(sum(pay_amnt),2) as cost_amnt_2w,
        count(distinct order_id) as order_amnt_2w,
        count(distinct catgroup_id) as buy_grps_2w
from bimining.member_churn_order_info 
where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),14),"-","") 
      and pay_date<='${hivevar:statis_date}'
      and bill_type=1     
GROUP BY member_id,dept_cd;

-----------step 8 过去1周购买特征汇总----------------  
drop table if exists bimining.member_churn_order_info_007;
CREATE TABLE bimining.member_churn_order_info_007 stored as orc as 
select  member_id,
        dept_cd,                      
        count(distinct pay_date) as buy_days_1w,
        round((datediff(from_unixtime(to_unix_timestamp(max(pay_date),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(to_unix_timestamp(min(pay_date),'yyyyMMdd'),'yyyy-MM-dd')))/7,2) as time_span_buy_1w,
        round(sum(pay_amnt),2) as cost_amnt_1w,
        count(distinct order_id) as order_amnt_1w,
        count(distinct catgroup_id) as buy_grps_1w
from bimining.member_churn_order_info 
where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),7),"-","") 
      and pay_date<='${hivevar:statis_date}'
      and bill_type=1     
GROUP BY member_id,dept_cd;

-----------step 9 过去1年退货特征汇总----------------  
drop table if exists bimining.member_churn_order_info_008;
CREATE TABLE bimining.member_churn_order_info_008 stored as orc as 
select t1.member_id,
		t1.dept_cd,
		t1.return_amnt_1y,
		t9.return_amnt_6m,
		t10.return_amnt_3m,
		t11.return_amnt_1m,
		t12.return_amnt_3w,
		t13.return_amnt_2w,
		t14.return_amnt_1w
from (
		select member_id,
                dept_cd,
                count(1) as return_amnt_1y
        from bimining.member_churn_order_info 
        where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),365),"-","") 
               and pay_date<='${hivevar:statis_date}'
               and bill_type=-1
        group by member_id,dept_cd      
       ) t1 
left join (
        select member_id,
                dept_cd,
                count(1) as return_amnt_6m
        from bimining.member_churn_order_info 
        where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),180),"-","") 
               and pay_date<='${hivevar:statis_date}'
               and bill_type=-1 
        group by member_id,dept_cd      
       ) t9
on t1.member_id=t9.member_id and t1.dept_cd=t9.dept_cd 
left join (
        select member_id,
                dept_cd,
                count(1) as return_amnt_3m
        from bimining.member_churn_order_info 
        where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),90),"-","") 
               and pay_date<='${hivevar:statis_date}'
               and bill_type=-1
        group by member_id,dept_cd      
       ) t10
on t1.member_id=t10.member_id and t1.dept_cd=t10.dept_cd        
left join (
        select member_id,
                dept_cd,
                count(1) as return_amnt_1m
        from bimining.member_churn_order_info 
        where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),30),"-","") 
               and pay_date<='${hivevar:statis_date}'
               and bill_type=-1
        group by member_id,dept_cd      
       ) t11
on t1.member_id=t11.member_id and t1.dept_cd=t11.dept_cd    
left join (
        select member_id,
                dept_cd,
                count(1) as return_amnt_3w
        from bimining.member_churn_order_info 
        where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),21),"-","") 
               and pay_date<='${hivevar:statis_date}'
               and bill_type=-1
        group by member_id,dept_cd      
       ) t12
on t1.member_id=t12.member_id and t1.dept_cd=t12.dept_cd  
left join (
        select member_id,
                dept_cd,
                count(1) as return_amnt_2w
        from bimining.member_churn_order_info 
        where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),14),"-","") 
               and pay_date<='${hivevar:statis_date}'
               and bill_type=-1
        group by member_id,dept_cd      
       ) t13
on t1.member_id=t13.member_id and t1.dept_cd=t13.dept_cd  
left join (
        select member_id,
                dept_cd,
                count(1) as return_amnt_1w
        from bimining.member_churn_order_info 
        where pay_date>=regexp_replace(date_sub(from_unixtime(to_unix_timestamp('${hivevar:statis_date}','yyyymmdd'),'yyyy-mm-dd'),7),"-","") 
               and pay_date<='${hivevar:statis_date}'
               and bill_type=-1 
        group by member_id,dept_cd      
       ) t14
on t1.member_id=t14.member_id and t1.dept_cd=t14.dept_cd; 



-----------step 10 购买特征汇总---------------- 



insert overwrite table bimining.member_churn_pruchase_feature partition (statis_date='${hivevar:statis_date}')
select t1.member_id,			--会员编码
		t1.dept_cd,				--部门编码
		t1.recent_buy,			--最近一次购买距今天数
		t1.buy_days_1y,			--最近一年购买天数
		t1.time_span_buy_1y,	--最近一年购买时间跨度占比
		t1.cost_amnt_1y,		--最近一年购买金额
		t1.order_amnt_1y,		--最近一年订单总数
		t1.buy_grps_1y,			--最近一年购买商品组数
		t2.buy_days_6m,			--最近6个月购买天数
		t2.time_span_buy_6m,	--最近6个月购买时间跨度占比
		t2.cost_amnt_6m,		--最近6个月购买金额
		t2.order_amnt_6m,		--最近6个月订单总数
		t2.buy_grps_6m,			--最近6个月购买商品组数
		t3.buy_days_3m,			--最近3个月购买天数
		t3.time_span_buy_3m,	--最近3个月购买时间跨度占比
		t3.cost_amnt_3m,		--最近3个月购买金额
		t3.order_amnt_3m,		--最近3个月订单总数
		t3.buy_grps_3m,			--最近3个月购买商品组数
		t4.buy_days_1m,			--最近1个月购买天数
		t4.time_span_buy_1m,	--最近1个月购买时间跨度占比
		t4.cost_amnt_1m,		--最近1个月购买金额
		t4.order_amnt_1m,		--最近1个月订单总数
		t4.buy_grps_1m,			--最近1个月购买商品组数
		t5.buy_days_3w,			--最近3周购买天数
		t5.time_span_buy_3w,	--最近3周购买时间跨度占比
		t5.cost_amnt_3w,		--最近3周购买金额
		t5.order_amnt_3w,		--最近3周订单总数
		t5.buy_grps_3w,			--最近3周购买商品组数
		t6.buy_days_2w,			--最近2周购买天数
		t6.time_span_buy_2w,	--最近2周购买时间跨度占比
		t6.cost_amnt_2w,		--最近2周购买金额
		t6.order_amnt_2w,		--最近2周订单总数
		t6.buy_grps_2w,			--最近2周购买商品组数
		t7.buy_days_1w,			--最近1周购买天数
		t7.time_span_buy_1w,	--最近1周购买时间跨度占比
		t7.cost_amnt_1w,		--最近1周购买金额
		t7.order_amnt_1w,		--最近1周订单总数
		t7.buy_grps_1w,			--最近1周购买商品组数
		t8.return_amnt_1y,		--最近1年退货数
		t8.return_amnt_6m,		--最近6个月退货数
		t8.return_amnt_3m,		--最近3个月退货数
		t8.return_amnt_1m,		--最近1个月退货数
		t8.return_amnt_3w,		--最近3周退货数
		t8.return_amnt_2w,		--最近2周退货数
		t8.return_amnt_1w		--最近1周退货数
from bimining.member_churn_order_info_001 t1
left join bimining.member_churn_order_info_002 t2
on t1.member_id=t2.member_id and t1.dept_cd=t2.dept_cd
left join bimining.member_churn_order_info_003 t3
on t1.member_id=t3.member_id and t1.dept_cd=t3.dept_cd
left join bimining.member_churn_order_info_004 t4
on t1.member_id=t4.member_id and t1.dept_cd=t4.dept_cd
left join bimining.member_churn_order_info_005 t5
on t1.member_id=t5.member_id and t1.dept_cd=t5.dept_cd
left join bimining.member_churn_order_info_006 t6
on t1.member_id=t6.member_id and t1.dept_cd=t6.dept_cd
left join bimining.member_churn_order_info_007 t7
on t1.member_id=t7.member_id and t1.dept_cd=t7.dept_cd
left join bimining.member_churn_order_info_008 t8
on t1.member_id=t8.member_id and t1.dept_cd=t8.dept_cd;