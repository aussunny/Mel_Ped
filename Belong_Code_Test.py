# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
import boto3
from sodapy import Socrata
##################################################Get Data###############################################
# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
#client = Socrata("data.melbourne.vic.gov.au", None) this way only limited 2000 rows can be download
client = Socrata("data.melbourne.vic.gov.au", 'xxxxxxxxxx', 
                 username = 'xxxxxxx@gmail.com',
                password = 'xxxxxxxx')

# Example authenticated client (needed for non-public datasets):
# client = Socrata(data.melbourne.vic.gov.au,
#                  MyAppToken,
#                  userame="user@example.com",
#                  password="AFakePassword")

# Have to use a limit to get all data, checked on the original dataset limit = 3860331 rows
# dictionaries by sodapy.
results_count = client.get("b2ak-trbp", limit=3860331)
results_location = client.get("h57g-5234")

# Convert to pandas DataFrame

count_df = pd.DataFrame.from_records(results_count)
location_df = pd.DataFrame.from_records(results_location)
##################################################Clean Data#############################################
# Clean dataset
count_df.drop_duplicates(inplace = True)
count_df=count_df.dropna()

# retrieve the date
c_df = count_df.drop(['day', 'id', 'mdate','sensor_name', 'time'], axis = 1)
c_df['Day'] = [x[0:10] for x in c_df['date_time']]
c_df = c_df.drop(['date_time'], axis = 1)
c_df['hourly_counts'] = pd.to_numeric(c_df['hourly_counts'])
c_df.rename(columns={'hourly_counts': 'Hourly_Counts', 'month':'Month','sensor_id':'Sensor_ID', 'year':'Year'}, inplace = True)

# sort data by date
c_df = c_df.sort_values('Day')

# prep location info
location_df.rename(columns={'sensor_id':'Sensor_ID', 'location':'Location','sensor_description':'Sensor_Description', 'latitude':'Latitude', 'longitude':'Longitude'}, inplace = True)

##########################################################Data Analysis for Monthly count#####################################################
# group the records by the month and year, sum the pedestrian for each sensor id
df_m = c_df.groupby(['Year','Month','Sensor_ID']).sum().reset_index()

# take the top 10 sensor counting per month and flatten the dataframe
df_mf = df_m.groupby(['Year','Month']).apply(lambda x : x.sort_values(by = 'Hourly_Counts', ascending = False).head(10).reset_index(drop=True))
df_mf = df_mf.drop(['Year','Month'], axis = 1).reset_index()

#add the location info
df_mf.rename(columns={"Hourly_Counts":"Monthly_Counts","level_2":"Monthly_Top10"}, inplace = True)
df_mf = df_mf.merge(location_df[['Sensor_ID', 'Location','Sensor_Description','Latitude','Longitude']], 'left')

# store Ped Month count data into month_count.csv
df_mf.to_csv('month_count.csv', index = False)

##########################################################Data Analysis for Daily count#####################################################
# Top 10 location by Day, remove the columns for year and month
c1_df = c_df.drop(['Year','Month'], axis = 1)

# group the records by the day, sum the pedestrian for each sensor id
df_d = c1_df.groupby(['Day','Sensor_ID']).sum().reset_index()

# take the top 10 sensor counting per day and flatten the dataframe
df_df = df_d.groupby(['Day']).apply(lambda x : x.sort_values(by = 'Hourly_Counts', ascending = False).head(10).reset_index(drop=True))
df_df = df_df.drop(['Day'], axis = 1).reset_index()

#add the location info
df_df.rename(columns={"Hourly_Counts":"Daily_Counts","level_1":"Daily_Top10"}, inplace = True)
df_df = df_df.merge(location_df[['Sensor_ID', 'Location','Sensor_Description','Latitude','Longitude']], 'left')

# store Ped day count data into day_count.csv
df_df.to_csv('day_count.csv', index = False)

##########################################################Upload Data To S3#####################################################
# upload csv files to s3
# configure your own access key and secret for AWS CLI and replace the bucket name below with your own bucket
s3 = boto3.resource('s3')    
s3.Bucket('mel-ped-count').upload_file('day_count.csv','test/day_ped_count.csv')
s3.Bucket('mel-ped-count').upload_file('month_count.csv','test/month_ped_count.csv')