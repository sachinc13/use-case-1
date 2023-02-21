import os,io
import sys
import pyspark
import boto3
import pandas as pd
from datetime import date
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# athena_client=boto3.client('athena',region_name="ap-south-1")
# s3_client=boto3.client('s3')
# s3=boto3.resource('s3')
# glue_client=boto3.client('glue',region_name="ap-south-1")
# s3_bucket_name='saama-gene-training-data-bucket'
# my_bucket=s3.Bucket(s3_bucket_name)
# inbound_path = 'sachinc/Inbound/'
# preprocess_path = 'sachinc/Preprocess/'
# landing_path = 'sachinc/Landing/'
# DatabaseName='saama-gene-training-data'
# xls_file='.xls'
# csv_file='.csv'

#inbound to preprocess
def copy_file(source_dir,target_dir):
    for file in my_bucket.objects.filter(Prefix=source_dir):
        print('1 file.key',file.key)
        file1=file.key
        print('2 file1',file1)
        if file1.endswith((xls_file,csv_file)) is True:
            print("Start coping the file.")
            destfile = file1.replace(source_dir,target_dir,1)
            copy_source = {'Bucket': s3_bucket_name, 'Key': file1}
            s3_client.copy_object(Bucket=s3_bucket_name, CopySource=copy_source, Key=destfile)
            print(f'File copied to {target_dir} location')
            
            
def cnvrt_xls_csv():
    for file in my_bucket.objects.filter(Prefix=preprocess_path):
        print('1',file.key)
        file1=file.key
        print('2',file1)
        if file1.endswith(xls_file) is True:
            extn = file1.split('.')[-2]
            print(extn)
            print("xls file exist...")
            read_file = pd.read_excel(f's3://{s3_bucket_name}/{file1}',header=0)
            print(read_file)
            read_file.to_csv(f's3://{s3_bucket_name}/{extn}.csv',index = None,header=True,sep='|')
            print("File converted to csv ....")
            
            
# #preprocess to landing
def move_to_landing(source_dir,target_dir):
    print('1')
    df_header = spark.read.options(delimiter = '|', header = 'False').csv('s3://' + s3_bucket_name + '/' + inbound_path + '/' + 'HEADER.txt')
    print('1.1')
    df_count = spark.read.options(header = 'False', inferSchema='True' ).csv('s3://' + s3_bucket_name + '/' + inbound_path + '/' + 'COUNTER.txt')
    print('1.2')
    for file in my_bucket.objects.filter(Prefix=source_dir):
        print('2',file.key)
        file1=file.key
        print('3',file1)
        if(file1.endswith(csv_file)) is True:
            print('4',file1)
            df_file_header = spark.read.options(delimiter = ',', header = 'False', quote ="\"").csv('s3://' + s3_bucket_name + '/' + inbound_path + '/' + 'SBIN.csv')
            print('5df_file_header')
            df_file_header.show()
            #check header and record count match
            if( (df_file_header.first() == df_header.first()) & (df_count.head()[0] ==  (df_file_header.count()-1)) ):
                destfile = file1.replace(source_dir,target_dir,1)
                copy_source = {'Bucket': s3_bucket_name, 'Key': file1}
                s3_client.copy_object(Bucket=s3_bucket_name, CopySource=copy_source, Key=destfile)
                
                
# def create_crawler1():
#     print('Start to Create crawler!...')
#     for file in my_bucket.objects.filter(Prefix=landing_path):
#         print('##Start to Create crawler!...')
#         file1=file.key
#         print('1',file1)
#         if(file1.endswith('.csv')) is True:
#             print('2',file1)
#             file_path = file1.split('.')[-2][0:-4]
#             print('3',file_path)
#             file_name = file1.split('/')[2]
#             print('4',file_name)
#             file = file_name.split('.')[-2]
#             print('5',file)
#             crawler_name=f'saama-gene-training-sachin-assign2-{file_name}'
#             print('6',crawler_name)
#             crawler_details = glue_client.list_crawlers()
#             print('7',crawler_details)
#             if crawler_name not in crawler_details['CrawlerNames']:
#                 response = glue_client.create_crawler(
#                     Name=crawler_name,
#                     Role='saama-gene-training-glue-service-role',
#                     DatabaseName='saama-gene-training-data',
#                     Targets={
#                         'S3Targets': [
#                             {
#                                 'Path': f's3://{s3_bucket_name}/{file_path}',
#                             }
#                         ]
#                     },
#                     TablePrefix=f'saama-gene-training-sachin_u1_'
#                 )
#                 print(f"{crawler_name}  - Crawler is created successfully....")
#             run_crawler(crawler_name)
            
def stand_create_crawler():
    print('Start to Create crawler!...')
    for file in my_bucket.objects.filter(Prefix=stand_path):
        print('##Start to Create crawler!...')
        file1=file.key
        print('1',file1)
        if(file1.endswith('.csv')) is True:
            print('2',file1)
            CrawlerName = 'saama-gene-training-sachin-crawler-u1'+'sand_df'
            print('CrawlerName->',CrawlerName)
            path = "s3://"+s3_bucket_name+"/"+stand_path
            print(path)
            crawler_details=glue_client.list_crawlers()
            if CrawlerName not in crawler_details['CrawlerNames']:
                response = glue_client.create_crawler(
                    Name=CrawlerName,
                    Role='saama-gene-training-glue-service-role',
                    DatabaseName='saama-gene-training-data',
                    Targets={
                        'S3Targets': [
                            {
                                'Path':path,
                            }
                        ]
                    },
                    TablePrefix=f'saama-gene-training-sachin_u1_stand_df_'
                )
                print(f"{CrawlerName}  - Crawler is created successfully....")
            run_crawler(CrawlerName)         
            
def run_crawler(crawler_name):
    response = glue_client.start_crawler(
        Name=crawler_name
        )
    print("Successfully started crawler")

def create_df(landing_path):
    for file in my_bucket.objects.filter(Prefix=landing_path):
        file1=file.key
        if(file1.endswith(csv_file)) is True:
            # print(file1)
            data=f's3://{s3_bucket_name}/{file1}'
            # print(data)
            df=spark.read.csv('s3://saama-gene-training-data-bucket/sachinc/Landing/SBIN.csv',header='True',inferSchema='True')
            # print('1',data)
            # df.show()
            df = df.select([f.col(col).alias(col.replace(' ', '_')) for col in df.columns])
            
            df=df.withColumn('Date',to_date(col('Date'),'M/d/yyyy'))\
                 .withColumn("Open",df.Open.cast(DoubleType()))\
                 .withColumn("High",df.High.cast(DoubleType()))\
                 .withColumn("Low",df.Low.cast(DoubleType()))\
                 .withColumn("Close",df.Close.cast(DoubleType()))
            # dataframe for standard report
            
            # df.show(5)
            # standard_path = "s3://"+s3_bucket_name+"/"+"sachinc/Standardized/"
            # s_file_path='s3://saama-gene-training-data-bucket/sachinc/Landing/SBIN.csv'.split("/")[-1].split(".")[0] 
            # df.repartition(1).write.options(header='True').csv(standard_path +s_file_path+'-stand_report_csv')
            
           
             
            full_window = Window.partitionBy([weekofyear(df.Date),year(df.Date)])
            New_df=df.withColumn('week_no',concat(lit('Week'),weekofyear(df.Date)))\
                     .withColumn('st_date',date_format(f.first(df.Date).over(full_window),'dd-MM-yyyy'))\
                     .withColumn('end_date',date_format(f.last(df.Date).over(full_window),'dd-MM-yyyy'))\
                     .withColumn('open',f.first(df.Open).over(full_window))\
                     .withColumn('high',round(f.max(df.High).over(full_window),2))\
                     .withColumn('low',round(f.min(df.Low).over(full_window),2))\
                     .withColumn('close',round(f.last(df.Close).over(full_window),2))\
                     .withColumn('adj_close',round(f.avg(df.Adj_Close).over(full_window),2))\
                     .withColumn('volume',floor(f.sum(df.Volume).over(full_window)))\
                     .select("Scrip_name","week_no","st_date","end_date","open","high","low","close","adj_close","volume")
            
            
            # New_df.printSchema()
            # New_df.show(5)
            #dataframe for summary report
            
            source_file_path='s3://saama-gene-training-data-bucket/sachinc/Landing/SBIN.csv'.split("/")[-1].split(".")[0]    
            Outbound = "s3://"+s3_bucket_name+"/"+"sachinc/Outbound/"
            New_df.repartition(1).write.options(header='True').csv(Outbound +source_file_path+'-smry_report_csv')



def sum_create_crawler():
    sm='SBIN-smry_report_csv/'
    print('Start to Create crawler!...1',sum_path)
    for file in my_bucket.objects.filter(Prefix=sum_path):
        print('##Start to Create crawler!...2',sum_path)
        file1=file.key
        print('3',file1)
        if(file1.endswith('.csv')) is True:
            print('2',file1)
            CrawlerName = 'saama-gene-training-sachin-crawler-u1'+'sam_df'
            print('CrawlerName->',CrawlerName)
            path = "s3://"+s3_bucket_name+"/"+sum_path
            print(path)
            crawler_details=glue_client.list_crawlers()
            if CrawlerName not in crawler_details['CrawlerNames']:
                response = glue_client.create_crawler(
                    Name=CrawlerName,
                    Role='saama-gene-training-glue-service-role',
                    DatabaseName='saama-gene-training-data',
                    Targets={
                        'S3Targets': [
                            {
                                'Path':path,
                            }
                        ]
                    },
                    TablePrefix=f'saama-gene-training-sachin_u1_sam_df_'
                )
                print(f"{CrawlerName}  - Crawler is created successfully....")
            run_crawler(CrawlerName)

def run_crawler(crawler_name):
    response = glue_client.start_crawler(
        Name=crawler_name
        )
    print("Successfully started crawler")

def ind_check():
    for file in my_bucket.objects.filter(Prefix=inbound_path):
        key=file.key
    if key.endswith(('.ind')) is True:
        print("Indicator file is exist to start process")
        return 0
    else:
        print("file does not exist")
        return 1
 
    
    
if __name__ == "__main__":
    athena_client=boto3.client('athena',region_name="ap-south-1")
    s3_client=boto3.client('s3')
    s3=boto3.resource('s3')
    glue_client=boto3.client('glue',region_name="ap-south-1")
    s3_bucket_name='saama-gene-training-data-bucket'
    my_bucket=s3.Bucket(s3_bucket_name)
    inbound_path = 'sachinc/Inbound/'
    preprocess_path = 'sachinc/Preprocess/'
    landing_path = 'sachinc/Landing/'
    stand_path = "sachinc/Standardized/SBIN-stand_report_csv/"
    sum_path = "sachinc/Outbound/SBIN-smry_report_csv/"
    DatabaseName='saama-gene-training-data'
    xls_file='.xls'
    csv_file='.csv'
    if(ind_check()==0):
        # copy_file(inbound_path,preprocess_path)
        # cnvrt_xls_csv()
        # move_to_landing(preprocess_path,landing_path)
        # create_crawler()
        # ind_check()
        # create_df(landing_path)
        # stand_create_crawler()
        sum_create_crawler()
    else:
        print("Exit the program...")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    