#!/usr/bin/env python
# coding: utf-8


# In[1]:


# if get any error, try restarting kernel
import findspark, sys
findspark.init("/opt/cloudera/parcels/CDH/lib/spark")
from pyspark.sql import *
from pyspark import SparkConf
from pyspark.sql.functions import *

if __name__ == "__main__":
        conf = (SparkConf()
                .setMaster("yarn")
                .set("spark.yarn.queue","root.appanalytics")
                .set("spark.dynamicAllocation.minExecutors","1")
                .set("spark.dynamicAllocation.maxExecutors","10")
                .set("spark.executor.cores","4")
                .set("spark.executor.memory","5g")
                .set("spark.driver.memory", "4g")
                #.set("spark.executorEnv.JAVA_HOME", "/usr/java/jdk1.8.0_144/")
                #.set("spark.jars", "/opt/ojdbc7.jar") # When we want spark to communicate with some RDBMS, we need a compatible connector
                #.set("spark.driver.extraClassPath", "/opt/ojdbc7.jar") # same as above step, use both configrations
                .set("spark.ui.port","4063")
                .set("spark.kryoserializer.buffer.max.mb", "2047")
                .set("spark.rpc.message.maxSize", "1000")
                )

        spark = (SparkSession
                 .builder
                 .config(conf=conf)
                 .appName("sample_notebook")
                 .getOrCreate())

        print("spark session created!!")

        # set variables

        max_commit_diff = int(sys.argv[2])
        first_commit_add = int(sys.argv[1])

        # read required files
        log_df = spark.read.csv('git-log-all-process.csv', header = True , inferSchema = True)
        alias_df = spark.read.csv('emailAliases.csv', header = True , inferSchema = True)

        # filter select columns
        df1 = log_df.select('commit','author','date')

        # add year-month
        df1 = df1.withColumn('commit_month',date_format("date","yyyy-MM"))


        # join to gett eh author alias and convert date string to date type
        df2 = df1.join(alias_df,df1.author ==  alias_df.author,"inner").select('alias','commit',to_timestamp('date','yyyy-MM-dd HH:mm:ssXXX').alias('commit_date'),'commit_month')

        # get only distinct commits as multiuple rows are there for single commit, and we are intrested in time spent
        windowSpec  = Window.partitionBy("alias","commit_month","commit").orderBy("commit_date")
        df2 = df2.withColumn("row_number",row_number().over(windowSpec))

        # get previous commit dates for each commit on author and month level
        windowSpec  = Window.partitionBy("alias","commit_month").orderBy("commit_date")
        df3 = df2.filter('row_number == 1').withColumn("previous_commit_date",lag("commit_date",1).over(windowSpec))

        # get diff in second, minutes, and hours, for out purpose we are using hours

        df3 = df3.withColumn('CommitNumber',row_number().over(Window.partitionBy("alias").orderBy("commit_date")))
        df3 = df3.withColumn('DiffInSeconds',(unix_timestamp("commit_date") - unix_timestamp("previous_commit_date")))
        df3 = df3.withColumn('DiffInMinutes',round(col('DiffInSeconds')/60))
        df3 = df3.withColumn('DiffInHours',round(col('DiffInMinutes')/60))
        df4 = df3.select('alias','commit','commit_date','commit_month','previous_commit_date','commitNumber','DiffInHours')

        # where diff in commit is less than some threshold , consider them in same session
        df5 = df4.withColumn('threshold_flag',expr(f"CASE WHEN (DiffInHours is NULL) THEN 1 WHEN (DiffInHours < '{max_commit_diff}') THEN 0 ELSE 1 END" ))

        # create session flag, will help in identifying sesssions
        windowSpec  = (Window.partitionBy("alias","commit_month").orderBy("commitNumber").rowsBetween(Window.unboundedPreceding, 0))
        df6 = df5.withColumn('SessionNo', sum("threshold_flag").over(windowSpec))

        # add some hours to first commit
        df7 = df6.withColumn('TimeSpent', expr(f" CASE WHEN DiffInHours is NULL then '{first_commit_add}' when threshold_flag = 1 then DiffInHours else 0 END "))
        # Calculate TimeSpentMonthly using windows function, same can be done by group by
        
        windowSpec = Window.partitionBy("alias", "commit_month").orderBy("commit_date").rowsBetween(Window.unboundedPreceding, 0)
        df9 = df7.withColumn('TimeSpentMonthly', sum("TimeSpent").over(windowSpec))     .withColumn('rn', row_number().over(Window.partitionBy("alias", "commit_month").orderBy(col("commit_date").desc())))
        
        df10 = df9.filter('rn == 1').select('alias','commit_month','TimeSpentMonthly').orderBy('alias','commit_month')
        
        print(df10.show())

        print("End of Code")
        spark.stop()



