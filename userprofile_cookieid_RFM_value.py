#!/usr/bin/env python
# encoding: utf-8

"""
File: userprofile_cookieid_RFM_value.py
Date: 2018/10/01
submit command: 
spark-submit --master yarn --deploy-mode client --driver-memory 1g  --executor-memory 4g 
--executor-cores 2 --num-executors 100 userprofile_cookieid_RFM_value.py start-date
"""

# A111H008_001    重要价值用户
# A111H008_002    重要保持用户
# A111H008_003    重要发展用户
# A111H008_004    重要挽留用户
# A111H008_005    一般价值用户
# A111H008_006    一般保持用户
# A111H008_007    一般发展用户
# A111H008_008    一般挽留用户
# dim_user_info
# user_consume_info

from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import sys
import datetime

def main():
    start_date = sys.argv[1]
    start_date_str = str(start_date)
    format = "%Y%m%d"
    target_table = 'dw.profile_tag_cookie' 

    # 用户RFM维度数据 (用户最后一次购买时间非空)
    user_cookie_relation =  " select t.userid,                                                                       \
                                     t.cookie_id as cookieid                                                         \
                                from (                                                                               \
                                      select cookie_id,                                                              \
                                             userid,                                                                 \
                                             row_number() over(partition by cookie_id order by last_time,last_date,userid desc) as rank  \
                                        from dw.cookie_user_relation                                                 \
                                       where cookie_id is not null                                                   \
                                         and userid is not null                                                      \
                                      ) t                                                                            \
                                where t.rank =1 "


    insert_table = "insert overwrite table " + target_table + " partition(data_date="+"'"+start_date_str+"'"+",tagtype='cookie_rfm_model') \
                            select case when t1.tagid = 'A111U008_001' then 'A111H008_001'    \
                                        when t1.tagid = 'A111U008_002' then 'A111H008_002'    \
                                        when t1.tagid = 'A111U008_003' then 'A111H008_003'    \
                                        when t1.tagid = 'A111U008_004' then 'A111H008_004'    \
                                        when t1.tagid = 'A111U008_005' then 'A111H008_005'    \
                                        when t1.tagid = 'A111U008_006' then 'A111H008_006'    \
                                        when t1.tagid = 'A111U008_007' then 'A111H008_007'    \
                                   else 'A111H008_008' end as tagid,         \
                                   t2.cookieid,                              \
                                   '' as tagweight,                          \
                                   '' as tagranking,                         \
                                   '' as reserve,                            \
                                   '' as reserve1                            \
                              from (                                         \
                                     select userid,                          \
                                            tagid                            \
                                       from dw.dw_profile_tag_user_userid             \
                                      where data_date = "+"'"+start_date_str+"'"+"    \
                                        and tagtype = 'rfm_model'                     \
                                    ) t1                                              \
                        inner join user_cookie_relation t2                            \
                                on t1.userid = t2.userid                              \
                          group by case when t1.tagid = 'A111U008_001' then 'A111H008_001'    \
                                        when t1.tagid = 'A111U008_002' then 'A111H008_002'    \
                                        when t1.tagid = 'A111U008_003' then 'A111H008_003'    \
                                        when t1.tagid = 'A111U008_004' then 'A111H008_004'    \
                                        when t1.tagid = 'A111U008_005' then 'A111H008_005'    \
                                        when t1.tagid = 'A111U008_006' then 'A111H008_006'    \
                                        when t1.tagid = 'A111U008_007' then 'A111H008_007'    \
                                   else 'A111H008_008' end,                                   \
                                   t2.cookieid  "

    spark = SparkSession.builder.appName("cookie_rfm_model").enableHiveSupport().getOrCreate()
    returned_df1 = spark.sql(user_cookie_relation).cache()
    returned_df1.createTempView("user_cookie_relation")
    
    spark.sql(insert_table)
      
if __name__ == '__main__':
    main()

