#!/usr/bin/env python
# encoding: utf-8

"""
File: userprofile_userid_paidinfo.py
Date: 2018/10/01
submit command: 
submit command: 
spark-submit --master yarn --deploy-mode client --driver-memory 1g  --executor-memory 2g 
--executor-cores 2 --num-executors 30 userprofile_userid_paidinfo.py start-date

A220U083_001   累计购买金额 
A220U084_001   最近一次购买距今天数
A220U087_001   累计购买次数
A220U088_001   注册未购买
"""

from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import sys
import datetime

def main():
    start_date = sys.argv[1]
    start_date_str = str(start_date)
    format_1 = "%Y%m%d"
    format_2 = "%Y-%m-%d"
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    old_date_partition = strftime(strptime(start_date_str, format_1), format_2)
    target_table = 'dw.profile_tag_user' 

    # 累计购买金额
    insert_all_paid_money = " insert overwrite table " + target_table + " partition(data_date="+"'"+start_date_str+"'"+",tagtype='userid_all_paid_money') \
                         select 'A220U083_001' as tagid,                      \
                                user_id as userid,                            \
                                sum(order_total_amount) as tagweight,         \
                                '' as tagranking,                             \
                                '' as reserve,                                \
                                '' as reserve1                                \
                           from dw.dw_order_fact                              \
                          where pay_status in (1,3)                           \
                       group by 'A220U083_001',user_id "
    
    # 累计购买次数
    insert_all_paid_times = " insert overwrite table " + target_table + " partition(data_date="+"'"+start_date_str+"'"+",tagtype='userid_all_paid_times') \
                         select 'A220U087_001' as tagid,                      \
                                user_id as userid,                            \
                                count(distinct order_id) as tagweight,        \
                                '' as tagranking,                             \
                                '' as reserve,                                \
                                '' as reserve1                                \
                           from dw.dw_order_fact                              \
                          where pay_status in (1,3)                           \
                       group by 'A220U087_001',user_id "


    # 最近一次购买距今天数
    insert_last_paid_days = " insert overwrite table " + target_table + " partition(data_date="+"'"+start_date_str+"'"+",tagtype='userid_last_paid') \
                         select 'A220U084_001' as tagid,                                                                      \
                                t.user_id as userid,                                                                          \
                                datediff(to_date("+"'"+old_date_partition+"'"+"),concat(substr(t.result_pay_time,1,10))) as tagweight,     \
                                '' as tagranking,                                                                             \
                                '' as reserve,                                                                                \
                                '' as reserve1                                                                                \
                           from (                                                                                             \
                                 select user_id,                                                                              \
                                        result_pay_time,                                                                      \
                                        row_number() over(partition by user_id order by result_pay_time desc) as rank         \
                                   from dw.dw_order_fact                                                                      \
                                  where pay_status in (1,3)                                                                   \
                                ) t                                                                                           \
                           where t.rank =1                                                                                    \
                        group by 'A220U084_001',t.user_id,                                                                    \
                                 datediff(to_date("+"'"+old_date_partition+"'"+"),concat(substr(t.result_pay_time,1,10)))"
    
    # 注册未购买
    regist_notpaid = " insert overwrite table " + target_table + " partition(data_date="+"'"+start_date_str+"'"+",tagtype='userid_regist_notpaid') \
                          select 'A220U088_001' as tagid,                     \
                                 user_id as userid,                           \
                                 '' as tagweight,                             \
                                 '' as tagranking,                            \
                                 '' as reserve,                               \
                                 '' as reserve1                               \
                            from dim.dim_user_info                            \
                           where data_date = "+"'"+start_date_str+"'"+"       \
                             and (paid_order_amount = 0 or paid_order_amount is null  )  \
                        group by 'A220U088_001', user_id  "
    
    
    spark = SparkSession.builder.appName("userid_paidinfo").enableHiveSupport().getOrCreate()
    spark.sql(insert_all_paid_money)
    spark.sql(insert_all_paid_times)
    spark.sql(insert_last_paid_days)
    spark.sql(regist_notpaid)
      
if __name__ == '__main__':
    main()

