import boto3
import awswrangler as wr
s3 = boto3.resource('s3')
data = wr.s3.read_csv('s3://mel-ped-count/test/day_ped_count.csv') 
print(data)