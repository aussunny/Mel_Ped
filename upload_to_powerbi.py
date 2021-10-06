import boto3
import awswrangler as wr
# configure AWS CLI to use boto3 with your own bucket
s3 = boto3.resource('s3')
# change to download the monthly count file data = wr.s3.read_csv('s3://mel-ped-count/test/month_ped_count.csv')
data = wr.s3.read_csv('s3://mel-ped-count/test/day_ped_count.csv') 
print(data)
