
#!/usr/bin/env python
# encoding: utf-8

"""
File: userprofile_userid_sms_blacklist.py
Date: 2018

submit command: 
spark-submit --master yarn --deploy-mode client --driver-memory 1g  --executor-memory 4g --executor-cores 2 
--num-executors 100 userprofile_userid_sms_blacklist.py start-date
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
    start_date_str_2 = strftime(strptime(start_date_str, format_1), format_2)
    old_date_partition_1 = strftime(strptime(start_date_str, format_1) - datetime.timedelta(1), format_1)
    week_day_ago_1 = strftime(strptime(start_date_str, format_1) - datetime.timedelta(14), format_1)
    week_day_ago_2 = strftime(strptime(start_date_str, format_1) - datetime.timedelta(14), format_2)

    target_table = 'dw.profile_tag_user'


    insertsql= "insert overwrite table " + target_table + "  partition (data_date="+"'"+start_date_str+"'"+",tagtype ='sms_blacklist')                         \
        select  'B120U037_003' as tagid,                                                                                                                      \
                nvl(t2.userid, t1.userid) as userid,                                                                                                            \
                '' as tagweight,                                                                                                                                \
                '' as tagranking,                                                                                                                               \
                '' as reserve,                                                                                                                                  \
                '' as reserve1                                                                                                                                  \
          from (                                                                                                                                                \
              select * from  dw.profile_tag_user  where data_date=" + "'"+old_date_partition_1+"'" +" and tagtype='sms_blacklist'                     \
                ) t1                                                                                                                                            \
    full outer join                                                                                                                                             \
              (                                                                                                                                                 \
                select p1.userid as userid                                                                                                                      \
                  from (                                                                                                                                        \
                        select cast(t.user_id as string) as userid                                                                                              \
                        from (                                                                                                                                  \
                                select user_id,                                                                                                                 \
                                       count(*) as num                                                                                                          \
                                  from dw.ods_responsys_SMS_DELIVERED                                                                                       \
                                 where status_code <> '0'                                                                                            \
                                   and to_date(event_captured_dt) >= "+ "'"+ week_day_ago_2 +"'"+"                                                              \
                                   and to_date(event_captured_dt) <= "+ "'"+ start_date_str_2 +"'"+"                                                            \
                              group by user_id                                                                                                                  \
                                having num >=3                                                                                                                  \
                              ) t                                                                                                                               \
                                                                                                                                                                \
                        union  all                                                                                                                              \
                        select cast(user_id as string) as userid                                                                                                \
                         from dw.ods_responsys_SMS_DELIVERED                                                                                                \
                        where status_code in ('1','3','9','10','12','15','21','10','19')                                                             \
                        ) p1                                                                                                                                    \
                ) t2                                                                                                                                            \
            on (t1.userid = t2.userid)                                                                                                                          \
       group by 'B120U037_0_003',                                                                                                                               \
                nvl(t2.userid, t1.userid),                                                                                                                      \
                '',                                                                                                                                             \
                '',                                                                                                                                             \
                '',                                                                                                                                             \
                ''                                  "
                
                
    spark = SparkSession.builder.appName("userprofile_userid_sms_blacklist").enableHiveSupport().getOrCreate()
    spark.sql(insertsql)
      
if __name__ == '__main__':
    main()





